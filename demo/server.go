package main

// Minimal HTTP backend to demonstrate MEST as backend with MPT (primary) and MEHT (secondary).
//
// Endpoints:
// - POST /api/upload-json    body: multipart file field "file" or raw JSON array/object
//   Behavior: for each JSON object, build primary key as "<contractAddress>/<tokenId>" and
//   insert (primaryKey -> serialized object) into MPT. Also insert into MEHT:
//     owner -> primaryKey, and for each trait's type/trait_type/key -> primaryKey.
// - GET /api/get?pk=...      returns the serialized JSON by primary key from MPT
// - GET /api/search?key=...  returns list of primary keys for a secondary key from MEHT
// - GET /health              simple health check
//
// Storage layout:
//   Primary LevelDB:   ./db/primary
//   Secondary LevelDB: ./db/secondary
//
// Notes:
// - This demo focuses on ingestion and simple query. Proof generation/verification APIs are
//   not exposed here, but the underlying engine supports them.
// - JSON shape is flexible. Field aliases are supported:
//     contractAddress: ["contractAddress", "合约地址", "contract"]
//     tokenId:         ["tokenId", "id", "token"]
//     owner:           ["owner", "Owner", "持有人"]
//     traits:          array of objects; trait type aliases: ["type", "trait_type", "key"]
//
// Build (from MEST/):
//   go run ./demo/server.go
// or
//   go build -o demo-server ./demo && ./demo-server

import (
    "bytes"
    "context"
    "crypto/tls"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "mime/multipart"
    "net"
    "net/http"
    "net/url"
    "os"
    "path/filepath"
    "os/signal"
    "strings"
    "sync"
    "syscall"
    "time"

    "github.com/Jalingpp/MEST/mpt"
    "github.com/Jalingpp/MEST/meht"
    "github.com/Jalingpp/MEST/util"
    "github.com/syndtr/goleveldb/leveldb"
)

type Server struct {
    primaryDB   *leveldb.DB
    secondaryDB *leveldb.DB
    primary     *mpt.MPT
    secondary   *meht.MEHT
    root        string
    persist     bool
    mu          sync.RWMutex
    startedAt   time.Time
}

func main() {
    s, err := newServer()
    if err != nil {
        log.Fatalf("failed to init server: %v", err)
    }

    mux := http.NewServeMux()
    mux.HandleFunc("/health", s.handleHealth)
    mux.HandleFunc("/api/upload-json", s.handleUploadJSON)
    mux.HandleFunc("/api/get", s.handleGetByPK)
    mux.HandleFunc("/api/search", s.handleSearch)
    mux.HandleFunc("/api/verify", s.handleVerify)
    // lightweight image proxy to avoid referrer/cert restrictions in browsers
    mux.HandleFunc("/img", s.handleImageProxy)

    addr := ":8080"
    log.Printf("MEST demo backend listening on %s", addr)
    // Cleanup hooks for ephemeral mode
    s.enableCleanupHooks()
    log.Fatal(http.ListenAndServe(addr, withCORS(mux)))
}

func newServer() (*Server, error) {
    root := filepath.Join(".", "demo")
    if err := os.MkdirAll(root, 0755); err != nil { return nil, err }
    primaryPath := filepath.Join(root, "db", "primary")
    secondaryPath := filepath.Join(root, "db", "secondary")
    if err := os.MkdirAll(primaryPath, 0755); err != nil { return nil, err }
    if err := os.MkdirAll(secondaryPath, 0755); err != nil { return nil, err }
    // ephemeral by default: clear previous run data unless MEST_DEMO_PERSIST=1
    persist := os.Getenv("MEST_DEMO_PERSIST") == "1"
    if !persist {
        _ = os.RemoveAll(primaryPath)
        _ = os.RemoveAll(secondaryPath)
        if err := os.MkdirAll(primaryPath, 0755); err != nil { return nil, err }
        if err := os.MkdirAll(secondaryPath, 0755); err != nil { return nil, err }
    }
    primaryDB, err := leveldb.OpenFile(primaryPath, nil)
    if err != nil { return nil, fmt.Errorf("open primary db: %w", err) }
    secondaryDB, err := leveldb.OpenFile(secondaryPath, nil)
    if err != nil { return nil, fmt.Errorf("open secondary db: %w", err) }

    // Init indexes
    // MPT cache capacity: defaults from SEDB for symmetry (small reasonable values)
    primary := mpt.NewMPT(primaryDB, true, int( sedbDefaultShortNodeCC() ), int( sedbDefaultFullNodeCC() ))
    // MEHT params
    mehtRdx := int( sedbDefaultMEHTRdx() )
    mehtBc := int( sedbDefaultMEHTBc() )
    mehtBs := int( sedbDefaultMEHTBs() )
    mehtWs := int( sedbDefaultMEHTWs() )
    mehtSt := int( sedbDefaultMEHTSt() )
    mehtBFsize := int( sedbDefaultMEHTBFsize() )
    mehtBFHnum := int( sedbDefaultMEHTBFHnum() )
    secondary := meht.NewMEHT(mehtRdx, mehtBc, mehtBs, mehtWs, mehtSt, mehtBFsize, mehtBFHnum, secondaryDB,
        int( sedbDefaultMgtNodeCC() ), int( sedbDefaultBucketCC() ), int( sedbDefaultSegmentCC() ), int( sedbDefaultMerkleTreeCC() ), true)

    return &Server{primaryDB: primaryDB, secondaryDB: secondaryDB, primary: primary, secondary: secondary, root: root, persist: persist, startedAt: time.Now()}, nil
}

// Defaults re-exposed to avoid importing sedb directly here
func sedbDefaultShortNodeCC() int { return 128 }
func sedbDefaultFullNodeCC() int  { return 128 }
func sedbDefaultMgtNodeCC() int   { return 256 }
func sedbDefaultBucketCC() int    { return 128 }
func sedbDefaultSegmentCC() int   { return 256 }
func sedbDefaultMerkleTreeCC() int { return 256 }
func sedbDefaultMEHTRdx() int     { return 16 }
func sedbDefaultMEHTBc() int      { return 1280 }
func sedbDefaultMEHTBs() int      { return 1 }
func sedbDefaultMEHTWs() int      { return 4 }
func sedbDefaultMEHTSt() int      { return 4 }
func sedbDefaultMEHTBFsize() int  { return 400000 }
func sedbDefaultMEHTBFHnum() int  { return 3 }

// handleHealth returns basic status
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
    writeJSON(w, http.StatusOK, map[string]any{
        "ok": true,
        "uptimeSec": int(time.Since(s.startedAt).Seconds()),
        "startAt": s.startedAt.Unix(),
    })
}

// handleUploadJSON ingests JSON array/object via multipart or raw body.
// Each object is inserted into MPT, and secondary keys (owner, trait types) into MEHT.
func (s *Server) handleUploadJSON(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
        return
    }
    ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
    defer cancel()
    r = r.WithContext(ctx)

    docs, err := readJSONBodies(r)
    if err != nil {
        http.Error(w, fmt.Sprintf("invalid body: %v", err), http.StatusBadRequest)
        return
    }
    type Result struct{
        Count int `json:"count"`
        PrimaryInserted int `json:"primaryInserted"`
        SecondaryInserted int `json:"secondaryInserted"`
        Errors []string `json:"errors"`
    }
    res := Result{}

    // process each uploaded doc individually so we can use its filename as a contract key
    for _, nd := range docs {
        fileKey := nd.Name // usually contract address without extension
        // Normalize this doc into mapping or items
        var (
            kvMap map[string]map[string]any
            items []map[string]any
        )
        if arr, ok := nd.Data.([]any); ok {
            for _, v := range arr { if m, ok := v.(map[string]any); ok { items = append(items, m) } }
        } else if mm, ok := nd.Data.(map[string]any); ok {
            if its, has := mm["items"]; has {
                if arr, ok := its.([]any); ok {
                    for _, v := range arr { if m2, ok := v.(map[string]any); ok { items = append(items, m2) } }
                }
            } else {
                isMapping := false
                for k, v := range mm { if _, ok := v.(map[string]any); ok && strings.Contains(k, "/") { isMapping = true; break } }
                if isMapping {
                    kvMap = make(map[string]map[string]any, len(mm))
                    for k, v := range mm { if m2, ok := v.(map[string]any); ok { kvMap[k] = m2 } }
                } else {
                    items = append(items, mm)
                }
            }
        }

        // list form
        for _, obj := range items {
            pk, err := derivePrimaryKey(obj)
            if err != nil { res.Errors = append(res.Errors, err.Error()); continue }
            // Optionally rewrite image URL to an accessible provider
            rewriteImageInObject(obj, pk)
            j, _ := json.Marshal(obj)
            s.insertPrimary(pk, string(j))
            res.PrimaryInserted++
            // owner/traits keys
            for _, sk := range deriveSecondaryKeys(obj) { s.insertSecondary(sk, pk); res.SecondaryInserted++ }
            // filename (contract) key
            if fileKey != "" { s.insertSecondary(fileKey, pk); res.SecondaryInserted++ } else { s.insertSecondary(contractFromPK(pk), pk); res.SecondaryInserted++ }
        }
        // mapping form
        for pk, obj := range kvMap {
            // Optionally rewrite image URL to an accessible provider
            rewriteImageInObject(obj, pk)
            j, _ := json.Marshal(obj)
            s.insertPrimary(pk, string(j))
            res.PrimaryInserted++
            for _, sk := range deriveSecondaryKeys(obj) { s.insertSecondary(sk, pk); res.SecondaryInserted++ }
            if fileKey != "" { s.insertSecondary(fileKey, pk); res.SecondaryInserted++ } else { s.insertSecondary(contractFromPK(pk), pk); res.SecondaryInserted++ }
        }
    }

    // Commit/update roots
    s.primary.MPTBatchFix(s.primaryDB)
    s.secondary.MGTBatchCommit(s.secondaryDB)

    writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleGetByPK(w http.ResponseWriter, r *http.Request) {
    pk := strings.TrimSpace(r.URL.Query().Get("pk"))
    if pk == "" {
        http.Error(w, "missing pk", http.StatusBadRequest)
        return
    }
    // MPT expects hex-encoded key
    val, _ := s.primary.QueryByKey(util.StringToHex(pk), s.primaryDB)
    if val == "" {
        writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
        return
    }
    var obj any
    if json.Unmarshal([]byte(val), &obj) == nil {
        writeJSON(w, http.StatusOK, obj)
        return
    }
    writeJSON(w, http.StatusOK, map[string]string{"value": val})
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
    key := strings.TrimSpace(r.URL.Query().Get("key"))
    if key == "" {
        http.Error(w, "missing key", http.StatusBadRequest)
        return
    }
    // MEHT expects hex-encoded key
    v, _, _, _, _ := s.secondary.QueryValueByKey(util.StringToHex(key), s.secondaryDB, false)
    if v == "" {
        writeJSON(w, http.StatusOK, map[string]any{"keys": []string{}})
        return
    }
    pks := strings.Split(v, ",")
    writeJSON(w, http.StatusOK, map[string]any{"keys": pks})
}

// handleImageProxy fetches an external image and streams it to the client.
// This helps when third-party CDNs block the referrer or the browser blocks the request.
// It only allows a small whitelist of hosts for safety.
func (s *Server) handleImageProxy(w http.ResponseWriter, r *http.Request) {
    raw := strings.TrimSpace(r.URL.Query().Get("u"))
    if raw == "" {
        http.Error(w, "missing u", http.StatusBadRequest)
        return
    }
    if strings.Contains(raw, "&amp;") { raw = strings.ReplaceAll(raw, "&amp;", "&") }
    u, err := url.Parse(raw)
    if err != nil || (u.Scheme != "https" && u.Scheme != "http") {
        http.Error(w, "bad url", http.StatusBadRequest)
        return
    }
    host := strings.ToLower(u.Host)
    if !isAllowedImageHost(host) {
        http.Error(w, "forbidden host", http.StatusForbidden)
        return
    }
    tr := &http.Transport{
        Proxy: http.ProxyFromEnvironment,
        DialContext: (&net.Dialer{ Timeout: 10 * time.Second, KeepAlive: 15 * time.Second }).DialContext,
        ForceAttemptHTTP2: true,
        TLSHandshakeTimeout: 10 * time.Second,
        ExpectContinueTimeout: 1 * time.Second,
        DisableKeepAlives: true,
        TLSClientConfig: &tls.Config{ ServerName: host, NextProtos: []string{"h2", "http/1.1"} },
    }
    client := &http.Client{ Timeout: 20 * time.Second, Transport: tr }
    req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, u.String(), nil)
    if err != nil { http.Error(w, "req error", http.StatusInternalServerError); return }
    req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    req.Header.Set("Accept", "image/avif,image/webp,image/apng,image/*,*/*;q=0.8")
    req.Header.Set("Accept-Language", "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7")
    req.Header.Set("Connection", "close")
    req.Header.Del("Referer")
    req.Header.Del("Origin")
    // (keep generic UA; do not force Referer to avoid mis-fetch)

    resp, err := client.Do(req)
    if err != nil || resp.StatusCode < 200 || resp.StatusCode > 299 {
        if resp != nil { _ = resp.Body.Close() }
        // Optional fallback via images.weserv.nl (enable by env MEST_DEMO_IMG_FALLBACK_WESERV=1)
        if envBool("MEST_DEMO_IMG_FALLBACK_WESERV") && (isOpenSeaHost(host) || strings.HasSuffix(host, "ipfs.io")) {
            alt := buildWeservURL(u.String())
            log.Printf("/img upstream failed for %s, trying weserv: %s", u.String(), alt)
            req2, err2 := http.NewRequestWithContext(r.Context(), http.MethodGet, alt, nil)
            if err2 == nil {
                req2.Header.Set("User-Agent", req.Header.Get("User-Agent"))
                req2.Header.Set("Accept", req.Header.Get("Accept"))
                req2.Header.Set("Accept-Language", req.Header.Get("Accept-Language"))
                req2.Header.Set("Connection", "close")
                resp2, err3 := client.Do(req2)
                if err3 == nil && resp2.StatusCode >= 200 && resp2.StatusCode <= 299 {
                    defer resp2.Body.Close()
                    if ct := resp2.Header.Get("Content-Type"); ct != "" { w.Header().Set("Content-Type", ct) } else { w.Header().Set("Content-Type", "image/jpeg") }
                    w.Header().Set("Cache-Control", "public, max-age=600")
                    w.WriteHeader(http.StatusOK)
                    _, _ = io.Copy(w, resp2.Body)
                    return
                } else if err3 != nil {
                    log.Printf("weserv fetch error: %v", err3)
                } else {
                    log.Printf("weserv upstream status: %s", resp2.Status)
                    _ = resp2.Body.Close()
                }
            }
        }
        if err != nil {
            log.Printf("upstream error for %s: %v", u.String(), err)
            http.Error(w, "upstream error", http.StatusBadGateway)
        } else {
            log.Printf("upstream status for %s: %s", u.String(), resp.Status)
            http.Error(w, "upstream status "+resp.Status, http.StatusBadGateway)
        }
        return
    }
    defer resp.Body.Close()
    if ct := resp.Header.Get("Content-Type"); ct != "" {
        w.Header().Set("Content-Type", ct)
    } else {
        w.Header().Set("Content-Type", "image/jpeg")
    }
    w.Header().Set("Cache-Control", "public, max-age=600")
    w.WriteHeader(http.StatusOK)
    _, _ = io.Copy(w, resp.Body)
}

func isAllowedImageHost(h string) bool {
    // minimal allowlist; extend as needed
    switch {
    case strings.HasSuffix(h, "i.seadn.io"),
         strings.HasSuffix(h, "openseauserdata.com"),
         strings.HasSuffix(h, "ipfs.io"),
         strings.HasSuffix(h, "cloudflare-ipfs.com"),
         strings.HasSuffix(h, "cf-ipfs.com"),
         strings.HasSuffix(h, "gateway.pinata.cloud"),
         strings.HasSuffix(h, "nftstorage.link"),
         strings.HasSuffix(h, "arweave.net"),
         strings.HasSuffix(h, "images.weserv.nl"):
        return true
    default:
        return false
    }
}

func isOpenSeaHost(h string) bool {
    return strings.HasSuffix(h, "i.seadn.io") || strings.HasSuffix(h, "openseauserdata.com")
}

func buildWeservURL(orig string) string {
    return "https://images.weserv.nl/?url=" + url.QueryEscape(orig)
}

func envBool(key string) bool {
    v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
    return v == "1" || v == "true" || v == "yes" || v == "on"
}

// rewriteImageInObject optionally replaces remote image URLs with a stable placeholder
// when MEST_DEMO_REWRITE_IMAGE=1 is set. This avoids external fetch failures in restricted networks.
func rewriteImageInObject(obj map[string]any, pk string) {
    if !envBool("MEST_DEMO_REWRITE_IMAGE") { return }
    // Prefer data URI when enabled: no外网依赖，最稳
    if envBool("MEST_DEMO_IMAGE_DATAURI") {
        repl := buildSVGDataURI(pk)
        // prefer to write under traits.image if traits is an object; else set top-level image
        if tv, ok := obj["traits"]; ok {
            if tm, ok2 := tv.(map[string]any); ok2 {
                tm["image"] = repl
                obj["traits"] = tm
                return
            }
        }
        obj["image"] = repl
        return
    }
    repl := os.Getenv("MEST_DEMO_IMAGE_URL")
    if strings.TrimSpace(repl) == "" {
        // default: placehold.co with overlay text showing "<contract>/<tokenId>"
        // example: https://placehold.co/600x600?text=0xabc...%2F1234
        repl = "https://placehold.co/600x600?text=" + url.QueryEscape(pk)
    }
    // prefer to write under traits.image if traits is an object; else set top-level image
    if tv, ok := obj["traits"]; ok {
        if tm, ok2 := tv.(map[string]any); ok2 {
            tm["image"] = repl
            obj["traits"] = tm
            return
        }
    }
    obj["image"] = repl
}

// buildSVGDataURI builds a visible SVG placeholder with the pk text embedded.
func buildSVGDataURI(pk string) string {
    // Simple dark tile with light text; fixed 600x600 for consistency.
    svg := `<?xml version="1.0" encoding="UTF-8"?>\n` +
        `<svg xmlns="http://www.w3.org/2000/svg" width="600" height="600" viewBox="0 0 600 600">` +
        `<rect width="600" height="600" fill="#0f172a"/>` +
        `<rect x="20" y="20" width="560" height="560" rx="16" ry="16" fill="#111827" stroke="#1f2937"/>` +
        `<text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle" fill="#cbd5e1" font-family="monospace" font-size="18">` +
        escapeForSVG(pk) + `</text>` +
        `</svg>`
    data := url.QueryEscape(svg)
    return "data:image/svg+xml;charset=UTF-8," + data
}

func escapeForSVG(s string) string {
    // minimal escaping for XML special chars
    s = strings.ReplaceAll(s, "&", "&amp;")
    s = strings.ReplaceAll(s, "<", "&lt;")
    s = strings.ReplaceAll(s, ">", "&gt;")
    return s
}

// insertPrimary inserts into primary MPT (pk -> value)
func (s *Server) insertPrimary(pk string, value string) {
    // Primary MPT requires keys as hex string (nibbles). Store raw JSON as value bytes.
    kv := util.KVPair{Key: util.StringToHex(pk), Value: value}
    s.primary.Insert(kv, s.primaryDB, nil, false)
}

// insertSecondary inserts into MEHT (secondary) with sk -> pk mapping
func (s *Server) insertSecondary(sk string, pk string) {
    // Secondary MEHT also expects hex-encoded key.
    s.secondary.Insert(util.KVPair{Key: util.StringToHex(sk), Value: pk}, s.secondaryDB, false, false)
}

// handleVerify syncs verification status for a list of primary keys.
// POST body: {"keys":["<addr>/<id>", ...]}
// GET  param: keys=pk1,pk2,... returns { map: { pk: bool }, verified: [] }
func (s *Server) handleVerify(w http.ResponseWriter, r *http.Request) {
    switch r.Method {
    case http.MethodPost:
        var req struct{ Keys []string `json:"keys"` }
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
            http.Error(w, "bad json", http.StatusBadRequest); return
        }
        if len(req.Keys) == 0 { writeJSON(w, http.StatusOK, map[string]any{"updated": 0}); return }
        batch := 0
        for _, pk := range req.Keys {
            if strings.TrimSpace(pk) == "" { continue }
            key := []byte("verify:" + pk)
            if err := s.secondaryDB.Put(key, []byte("1"), nil); err != nil {
                http.Error(w, "db error", http.StatusInternalServerError); return
            }
            batch++
        }
        writeJSON(w, http.StatusOK, map[string]any{"updated": batch})
    case http.MethodGet:
        keys := strings.Split(r.URL.Query().Get("keys"), ",")
        result := make(map[string]bool, len(keys))
        verified := make([]string, 0, len(keys))
        for _, pk := range keys {
            pk = strings.TrimSpace(pk)
            if pk == "" { continue }
            val, err := s.secondaryDB.Get([]byte("verify:"+pk), nil)
            ok := (err == nil && len(val) > 0)
            result[pk] = ok
            if ok { verified = append(verified, pk) }
        }
        writeJSON(w, http.StatusOK, map[string]any{"map": result, "verified": verified})
    default:
        http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
    }
}

// derivePrimaryKey builds "<contractAddress>/<tokenId>" with common aliases
func derivePrimaryKey(obj map[string]any) (string, error) {
    addr := pickString(obj, "contractAddress", "合约地址", "contract")
    if addr == "" { return "", fmt.Errorf("missing contractAddress/合约地址/contract") }
    token := pickString(obj, "tokenId", "id", "token")
    if token == "" { return "", fmt.Errorf("missing tokenId/id/token") }
    return fmt.Sprintf("%s/%s", addr, token), nil
}

// deriveSecondaryKeys returns list of keys: [owner, ...traitTypes]
func deriveSecondaryKeys(obj map[string]any) []string {
    out := make([]string, 0, 8)
    if owner := pickString(obj, "owner", "Owner", "持有人"); owner != "" {
        out = append(out, owner)
    }
    // traits can be array or object
    if ts, ok := obj["traits"]; ok {
        switch t := ts.(type) {
        case []any:
            // keep compatibility: use element's type/trait_type/key as key
            for _, v := range t {
                if m, ok := v.(map[string]any); ok {
                    if typ := pickString(m, "type", "trait_type", "key"); typ != "" {
                        out = append(out, typ)
                    }
                }
            }
        case map[string]any:
            // requirement: insert all keys under traits except image and name
            for k := range t {
                if !isImageOrNameField(k) {
                    out = append(out, k)
                }
            }
        }
    }
    return dedup(out)
}

func isImageOrNameField(s string) bool {
    switch strings.ToLower(s) {
    case "image", "image_url", "imageurl", "name":
        return true
    default:
        return false
    }
}

func pickString(m map[string]any, keys ...string) string {
    for _, k := range keys {
        if v, ok := m[k]; ok {
            switch vv := v.(type) {
            case string:
                return strings.TrimSpace(vv)
            case json.Number:
                return vv.String()
            case float64:
                return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%f", vv), "0"), ".")
            }
        }
    }
    return ""
}

func dedup(in []string) []string {
    if len(in) == 0 { return in }
    m := map[string]struct{}{}
    out := make([]string, 0, len(in))
    for _, s := range in {
        if s == "" { continue }
        if _, ok := m[s]; ok { continue }
        m[s] = struct{}{}
        out = append(out, s)
    }
    return out
}

func withCORS(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Access-Control-Allow-Origin", "*")
        w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")
        if r.Method == http.MethodOptions { return }
        next.ServeHTTP(w, r)
    })
}

func writeJSON(w http.ResponseWriter, code int, v any) {
    w.Header().Set("Content-Type", "application/json; charset=utf-8")
    w.WriteHeader(code)
    enc := json.NewEncoder(w)
    enc.SetEscapeHTML(false)
    _ = enc.Encode(v)
}

// readJSONBodies reads JSON from request. Supports:
// - multipart with one or more files (field name can be 'file' or any)
// - raw body JSON (single document)
type namedDoc struct { Name string; Data any }

func readJSONBodies(r *http.Request) ([]namedDoc, error) {
    ct := r.Header.Get("Content-Type")
    if strings.HasPrefix(ct, "multipart/form-data") {
        if err := r.ParseMultipartForm(512 << 20); err != nil { // up to 512MB in-memory + temp files
            return nil, err
        }
        var out []namedDoc
        // collect all file headers from all fields
        for _, fhs := range r.MultipartForm.File {
            for _, fh := range fhs {
                f, err := fh.Open()
                if err != nil { return nil, err }
                by, err := io.ReadAll(f)
                f.Close()
                if err != nil { return nil, err }
                by = bytes.TrimSpace(by)
                var v any
                if err := json.Unmarshal(by, &v); err != nil { return nil, err }
                name := strings.TrimSuffix(fh.Filename, filepath.Ext(fh.Filename))
                out = append(out, namedDoc{ Name: name, Data: v })
            }
        }
        if len(out) == 0 {
            return nil, fmt.Errorf("no files in multipart")
        }
        return out, nil
    }
    by, err := io.ReadAll(io.LimitReader(r.Body, 512<<20))
    if err != nil { return nil, err }
    defer r.Body.Close()
    by = bytes.TrimSpace(by)
    if len(by) == 0 { return nil, fmt.Errorf("empty body") }
    var v any
    if err := json.Unmarshal(by, &v); err != nil { return nil, err }
    return []namedDoc{{ Name: "", Data: v }}, nil
}

// enableCleanupHooks removes previous data on shutdown when in ephemeral mode.
func (s *Server) enableCleanupHooks() {
    if s.persist { return }
    sigs := make(chan os.Signal, 1)
    signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        <-sigs
        // close DBs before removing files
        if s.primaryDB != nil { _ = s.primaryDB.Close() }
        if s.secondaryDB != nil { _ = s.secondaryDB.Close() }
        dbRoot := filepath.Join(s.root, "db")
        _ = os.RemoveAll(dbRoot)
        os.Exit(0)
    }()
}

func readJSONBody(r *http.Request) (any, error) {
    ct := r.Header.Get("Content-Type")
    if strings.HasPrefix(ct, "multipart/form-data") {
        if err := r.ParseMultipartForm(64 << 20); err != nil { // 64MB
            return nil, err
        }
        // prefer field name "file"
        file, header, err := r.FormFile("file")
        if err != nil {
            // try any first file if "file" missing
            if r.MultipartForm != nil && len(r.MultipartForm.File) > 0 {
                for _, fhs := range r.MultipartForm.File {
                    if len(fhs) > 0 { file, header, err = openFirst(fhs); break }
                }
            }
        }
        if err != nil { return nil, fmt.Errorf("no file in multipart: %w", err) }
        defer file.Close()
        by, err := io.ReadAll(file)
        if err != nil { return nil, err }
        _ = header // currently unused
        var v any
        if err := json.Unmarshal(by, &v); err != nil { return nil, err }
        return v, nil
    }
    by, err := io.ReadAll(io.LimitReader(r.Body, 128<<20))
    if err != nil { return nil, err }
    defer r.Body.Close()
    // trim BOM/whitespace
    by = bytes.TrimSpace(by)
    var v any
    if err := json.Unmarshal(by, &v); err != nil { return nil, err }
    return v, nil
}

func openFirst(fhs []*multipart.FileHeader) (multipart.File, *multipart.FileHeader, error) {
    if len(fhs) == 0 { return nil, nil, fmt.Errorf("empty file list") }
    f, err := fhs[0].Open()
    return f, fhs[0], err
}

// contractFromPK extracts contract part from "<addr>/<id>"
func contractFromPK(pk string) string {
    if i := strings.Index(pk, "/"); i > 0 { return pk[:i] }
    return pk
}
