MEST Demo Backend

This demo server exposes a minimal HTTP API to ingest JSON data and index it with:
- Primary index: MPT (key = "合约地址/id", value = serialized JSON object)
- Secondary index: MEHT (keys = owner and each traits' type/trait_type/key; value = "合约地址/id")

Endpoints
- POST /api/upload-json
  - Accepts multipart form with field `file` (JSON) or raw JSON in body.
  - JSON can be an array or a single object. Also supports `{ "items": [ ... ] }`.
  - Field aliases:
    - contractAddress: 合约地址 | contract
    - tokenId: id | token
    - owner: owner | Owner | 持有人
    - traits type: type | trait_type | key
- GET /api/search?key=<owner-or-trait-type>
  - Returns primary keys (合约地址/id) associated with the secondary key via MEHT.
- GET /api/get?pk=<合约地址/id>
  - Returns the stored JSON object by primary key from MPT.
- GET /health

Run
- go run ./demo/server.go
  - Or: ./start.sh
- Server listens on :8080 and allows CORS for local dev.

Frontend Integration (mest-demo)
- Upload JSON to POST http://localhost:8080/api/upload-json with multipart field `file`.
- Query via GET http://localhost:8080/api/search?key=<owner-or-trait-type>.

