DUCKDB_VERSION=1.2.0 
wget https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/libduckdb-linux-amd64.zip \
    && unzip libduckdb-linux-amd64.zip -d libduckdb \
    && mv libduckdb/duckdb.* /usr/local/include \
    && mv libduckdb/libduckdb.so /usr/local/lib \
    && mv libduckdb/libduckdb*.* /usr/local/include \
    && rm -rf libduckdb *.zip