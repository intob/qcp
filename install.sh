#!/bin/sh
set -e
VERSION=$(git describe --tags --always --dirty)
go install -ldflags "-X main.version=$VERSION" .
echo "installed qcp $VERSION"
