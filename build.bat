@echo off
echo Building Bleep Proxy for multiple platforms...
echo.

if not exist build mkdir build

echo Building for Windows (amd64)...
set GOOS=windows
set GOARCH=amd64
go build -tags server -o build/server-windows-amd64.exe protocol.go common.go config.go server.go
go build -tags client -o build/client-windows-amd64.exe protocol.go common.go config.go client.go
echo Windows amd64 build completed!
echo.

echo Building for Windows (arm64)...
set GOOS=windows
set GOARCH=arm64
go build -tags server -o build/server-windows-arm64.exe protocol.go common.go config.go server.go
go build -tags client -o build/client-windows-arm64.exe protocol.go common.go config.go client.go
echo Windows arm64 build completed!
echo.

echo Building for Linux (amd64)...
set GOOS=linux
set GOARCH=amd64
go build -tags server -o build/server-linux-amd64 protocol.go common.go config.go server.go
go build -tags client -o build/client-linux-amd64 protocol.go common.go config.go client.go
echo Linux amd64 build completed!
echo.

echo Building for Linux (arm64)...
set GOOS=linux
set GOARCH=arm64
go build -tags server -o build/server-linux-arm64 protocol.go common.go config.go server.go
go build -tags client -o build/client-linux-arm64 protocol.go common.go config.go client.go
echo Linux arm64 build completed!
echo.

echo ========================================
echo Multi-platform build completed!
echo ========================================
echo.
echo Build files created in build/ directory
echo.
pause
