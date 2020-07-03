VERSION  := $(shell git tag | tail -n1 | sed 's/v//g')
REVISION := $(shell git rev-parse --short HEAD)
INFO_COLOR=\033[1;34m
RESET=\033[0m
BOLD=\033[1m

.DEFAULT_GOAL := build

.PHONY: build
## build: build the nke
build:
	go build -o binary/isaka -ldflags "-X github.com/pyama86/isaka/cmd.version=$(VERSION)-$(REVISION)"

.PHONY: release
## release: release nke (tagging and exec goreleaser)
release:
	git semv patch --bump
	goreleaser --rm-dist

.PHONY: releasedeps
releasedeps: git-semv goreleaser

.PHONY: git-semv
git-semv:
	brew tap linyows/git-semv
	brew install git-semv

.PHONY: goreleaser
goreleaser:
	brew install goreleaser/tap/goreleaser
	brew install goreleaser
