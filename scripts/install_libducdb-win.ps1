echo "This script will download "
$version = "1.2.1" 
$url = "https://github.com/duckdb/duckdb/releases/download/v$version/libduckdb-windows-amd64.zip"
$outputPath = "$env:USERPROFILE\Downloads\libduckdb-windows-amd64.zip"
$uncompressedPath = "$env:USERPROFILE\Downloads\libduckdb-windows-amd64"

Invoke-WebRequest -Uri $url -OutFile $outputPath
Expand-Archive -Path $outputPath -DestinationPath $uncompressedPath -Force