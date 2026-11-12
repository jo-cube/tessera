# The validation toolchain plus Debian's RocksDB, for the comparison in
# tools/compare only. Build with
#   docker --context colima build -t tessera-compare:rocksdb-7.8.3 - < tools/compare/Dockerfile
# and run through scripts/validate.sh with TESSERA_IMAGE set to that tag.
FROM tessera-validate:1.98.1
RUN apt-get update \
 && apt-get install -y --no-install-recommends librocksdb-dev=7.8.3-2 \
 && rm -rf /var/lib/apt/lists/*
