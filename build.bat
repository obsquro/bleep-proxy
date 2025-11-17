@echo off
echo Building Bleep Proxy for multiple platforms...
echo.

if not exist build mkdir build

echo Building for Windows (amd64)...
set GOOS=windows
set GOARCH=amd64
go build -tags server -o build/server-windows-amd64.exe protocol.go server.go
go build -tags client -o build/client-windows-amd64.exe protocol.go client.go
echo Windows build completed!
echo.

echo Building for Linux (amd64)...
set GOOS=linux
set GOARCH=amd64
go build -tags server -o build/server-linux-amd64 protocol.go server.go
go build -tags client -o build/client-linux-amd64 protocol.go client.go
echo Linux build completed!
echo.

echo Building for macOS (amd64)...
set GOOS=darwin
set GOARCH=amd64
go build -tags server -o build/server-darwin-amd64 protocol.go server.go
go build -tags client -o build/client-darwin-amd64 protocol.go client.go
echo macOS build completed!
echo.

echo Building for macOS (Apple Silicon)...
set GOOS=darwin
set GOARCH=arm64
go build -tags server -o build/server-darwin-arm64 protocol.go server.go
go build -tags client -o build/client-darwin-arm64 protocol.go client.go
echo macOS ARM64 build completed!
echo.

echo ========================================
echo Multi-platform build completed!
echo ========================================
echo.
echo Build files created in build/ directory
echo.
pause
