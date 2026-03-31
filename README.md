# samoa

[![CI Status](https://github.com/zoobz-io/samoa/workflows/CI/badge.svg)](https://github.com/zoobz-io/samoa/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/zoobz-io/samoa/graph/badge.svg?branch=main)](https://codecov.io/gh/zoobz-io/samoa)
[![Go Report Card](https://goreportcard.com/badge/github.com/zoobz-io/samoa)](https://goreportcard.com/report/github.com/zoobz-io/samoa)
[![CodeQL](https://github.com/zoobz-io/samoa/workflows/CodeQL/badge.svg)](https://github.com/zoobz-io/samoa/security/code-scanning)
[![Go Reference](https://pkg.go.dev/badge/github.com/zoobz-io/samoa.svg)](https://pkg.go.dev/github.com/zoobz-io/samoa)
[![License](https://img.shields.io/github/license/zoobz-io/samoa)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/zoobz-io/samoa)](go.mod)
[![Release](https://img.shields.io/github/v/release/zoobz-io/samoa)](https://github.com/zoobz-io/samoa/releases)

Template repository for standing up Go library infrastructure matching the zoobz-io ecosystem. Clone it, rename it, start building.

## What's Included

```
your-new-package/
├── go.mod                    # Go 1.24+ with toolchain
├── Makefile                  # test, lint, coverage, ci targets
├── .golangci.yml             # v2 config with security linters
├── .codecov.yml              # 70% project / 80% patch targets
├── .goreleaser.yml           # Library release automation
├── .github/workflows/
│   ├── ci.yml                # Test matrix, lint, security, benchmarks
│   ├── coverage.yml          # Codecov integration
│   ├── release.yml           # Tag-triggered releases
│   └── codeql.yml            # Security analysis
├── testing/
│   ├── helpers.go            # Reusable test utilities
│   ├── integration/          # Integration test structure
│   └── benchmarks/           # Benchmark structure
├── LICENSE                   # MIT
├── CONTRIBUTING.md           # Development workflow
└── SECURITY.md               # Vulnerability reporting
```

## Install

This is a GitHub template repository. Click **Use this template** on GitHub, or:

```bash
gh repo create your-package --template zoobz-io/samoa --clone
cd your-package
```

Then update `go.mod`, `Makefile`, and `.goreleaser.yml` with your package name.

## Quick Start

After creating your repository from the template:

```bash
# Update module path
sed -i 's/samoa/your-package/g' go.mod Makefile .goreleaser.yml

# Install development tools
make install-tools

# Verify everything works
make check

# Start building
```

## Capabilities

| Feature | Description | Configuration |
|---------|-------------|---------------|
| Multi-version testing | Go 1.24 and 1.25 matrix | `.github/workflows/ci.yml` |
| Linting | golangci-lint v2 with security focus | `.golangci.yml` |
| Coverage tracking | Codecov with PR comments | `.codecov.yml` |
| Release automation | GoReleaser for library packages | `.goreleaser.yml` |
| Security scanning | gosec + CodeQL | `.github/workflows/codeql.yml` |
| Benchmarks | Structured benchmark directory | `testing/benchmarks/` |

## Why Samoa?

- **Immediate CI**: Push and your workflows run—no setup required
- **Consistent tooling**: Every zoobz-io package uses the same linter config, coverage targets, and release process
- **Security by default**: gosec, CodeQL, and security-focused linters enabled from day one
- **Test infrastructure ready**: Helpers, integration tests, and benchmarks structured and waiting

## The Zoobz-io Ecosystem

All zoobz-io Go libraries use samoa as their foundation. When standards evolve, updates propagate from here.

## Documentation

- **Learn**: Review the configuration files directly—they're documented inline
- **Guides**: See `CONTRIBUTING.md` for development workflow
- **Reference**: Run `make help` for available commands

## Contributing

Development workflow and conventions are documented in [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT License - see [LICENSE](LICENSE)
