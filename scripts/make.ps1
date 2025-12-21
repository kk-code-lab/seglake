#!/usr/bin/env pwsh
<#
Helper to mirror common Make targets in PowerShell.
Usage: ./scripts/make.ps1 <target>
Targets: build, install, test, test-coverage, test-race, clean, fmt, lint, check, run, help.
Requires: Go in PATH; golangci-lint for lint.
#>

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('build', 'install', 'test', 'test-coverage', 'test-race', 'clean', 'fmt', 'lint', 'check', 'run', 'help')]
    [string]$Target = 'help',
    [string[]]$RunArgs = @(),
    [string]$RunArgsString = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot '..')
Push-Location $repoRoot
try {
    $binaryName = 'seglake'
    $buildDir = 'build'
    $exeSuffix = $IsWindows ? '.exe' : ''
    $binPath = Join-Path $buildDir "$binaryName$exeSuffix"
    $mainEntry = './cmd/seglake'
    $internalPackages = './internal/...'

    function Get-GitCommit {
        try {
            $result = git rev-parse --short HEAD 2>$null
            if ($result) { return $result.Trim() }
        } catch {
        }
        return 'unknown'
    }

    $buildFlag = "-X github.com/kk-code-lab/seglake/internal/app.BuildCommit=$(Get-GitCommit) -X github.com/kk-code-lab/seglake/internal/app.Version=dev"

    function Invoke-CommandChecked {
        param([Parameter(Mandatory)][string]$Description, [Parameter(Mandatory)][scriptblock]$Action)
        Write-Host $Description
        & $Action
    }

    if ($RunArgs.Count -eq 0 -and -not [string]::IsNullOrWhiteSpace($RunArgsString)) {
        $RunArgs = $RunArgsString -split '\s+'
    }

    switch ($Target) {
        'build' {
            Invoke-CommandChecked -Description "Building $binaryName..." -Action { go build -ldflags $buildFlag -o $binPath $mainEntry }
        }
        'install' {
            Invoke-CommandChecked -Description "Installing $binaryName to GOBIN (or GOPATH/bin)..." -Action { go install -ldflags $buildFlag $mainEntry }
        }
        'test' {
            Invoke-CommandChecked -Description 'Running tests...' -Action { go test $internalPackages }
        }
        'test-coverage' {
            Invoke-CommandChecked -Description 'Running tests with coverage...' -Action { go test $internalPackages -cover }
        }
        'test-race' {
            Invoke-CommandChecked -Description 'Running tests with race detector...' -Action { go test $internalPackages -race }
        }
        'clean' {
            Write-Host 'Cleaning build artifacts...'
            Remove-Item -Force -ErrorAction SilentlyContinue $binPath
        }
        'fmt' {
            Invoke-CommandChecked -Description 'Formatting code...' -Action { go fmt ./... }
        }
        'lint' {
            Invoke-CommandChecked -Description 'Linting code (requires golangci-lint)...' -Action { golangci-lint run ./... }
        }
        'check' {
            Invoke-CommandChecked -Description 'Linting code (requires golangci-lint)...' -Action { golangci-lint run ./... }
            Invoke-CommandChecked -Description "Building $binaryName..." -Action { go build -ldflags $buildFlag -o $binPath $mainEntry }
            Invoke-CommandChecked -Description 'Type-checking tests (no execution)...' -Action { go test -run '^$' $internalPackages }
        }
        'run' {
            Invoke-CommandChecked -Description "Building $binaryName..." -Action { go build -ldflags $buildFlag -o $binPath $mainEntry }
            Invoke-CommandChecked -Description "Running $binaryName..." -Action { & $binPath @RunArgs }
        }
        Default {
            Write-Host 'seglake - S3-compatible object store'
            Write-Host ''
            Write-Host 'Available targets:'
            Write-Host '  build              - Build the binary to build/seglake'
            Write-Host '  install            - Install the binary to GOBIN (or GOPATH/bin)'
            Write-Host '  test               - Run all tests'
            Write-Host '  test-coverage      - Run tests with coverage report'
            Write-Host '  test-race          - Run tests with race detector'
            Write-Host '  check              - Lint, build, and compile tests without running them'
            Write-Host '  run                - Build and run the application'
            Write-Host '  clean              - Remove build artifacts'
            Write-Host '  fmt                - Format code with go fmt'
            Write-Host '  lint               - Run linter (requires golangci-lint)'
            Write-Host '  help               - Show this help message'
        }
    }
}
finally {
    Pop-Location
}
