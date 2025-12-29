go run test_insertion.go mpt 10000 32 1000 9000 500 1 data/test_insert
sleep 1
go run test_query.go mpt 10000 32 9000 500 1 data/test_query
sleep 1
rm -rf data/levelDB
sleep 1
go run test_insertion.go mbt 10000 32 1000 9000 500 1 data/test_insert
sleep 1
go run test_query.go mbt 10000 32 9000 500 1 data/test_query
sleep 1
rm -rf data/levelDB
sleep 1
go run test_insertion.go meht 10000 32 1000 9000 500 1 data/test_insert
sleep 1
go run test_query.go meht 10000 32 9000 500 1 data/test_query
sleep 1
rm -rf data/levelDB