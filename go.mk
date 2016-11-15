#
# Generic Go project makefile defaults file
#
# Usage:
#
#   Renaming this file to "Makefile" and running "make" should just work.
#   But you can also override one or more recipes:
#
#   - Include this file with "include go.mk" as the first line of your custom Makefile
#   - Define recipes using SRCDIR, GOROOT, GCFLAGS, LDFLAGS
#
#   - Optionally you can:
#
#       - Configure names of binaries, by setting the "BINS" variable before the "include" line
#         (they are autodetected from cmd/* by default)
#
#       - Override the default recipes. To call the defaults, use the "<recipe>.default" syntax.
#         (non-existing recipes are tried with "<recipe>.default")
#
#
# Example:
# make PREFIX= DESTDIR=/tmp/project-name-build install
#
# PREFIX is mostly unused, it would be used to expose the installation path to installed files
# For now, running "make install" with PREFIX= is OK (as long as DESTDIR is set)
#
# For capistrano, it would be something like this:
#
# export DESTDIR=/tmp/project-name-build
# export REALPREFIX=/opt/go/project-name/production/releases/1478000000
# make PREFIX=$REALPREFIX build # Just build it
# make PREFIX= install # Install with PREFIX= so that we don't have /opt/go/... under $DESTDIR
# scp -R $DESTDIR/* user@host:$REALPREFIX/
#

SRCDIR ?= .
PREFIX ?= /usr/local
GOROOT ?= /usr/local/go

GITFLAGS ?= GIT_DIR=${SRCDIR}/.git GIT_WORK_TREE=${SRCDIR}
ifeq ($(NOGIT),1)
  GitSummary ?= Unknown
  GitBranch ?= Unknown
else
  GitSummary := $(shell ${GITFLAGS} git describe --tags --dirty --always)
  GitBranch := $(shell ${GITFLAGS} git symbolic-ref -q --short HEAD)
endif

# Determine commands by looking into cmd/*
COMMANDS ?= $(wildcard ${SRCDIR}/cmd/*)

# Determine binary names by stripping out the dir names
BINS ?= $(foreach cmd,${COMMANDS},$(notdir ${cmd}))

ifeq (${BINS},)
  $(error Could not determine BINS, set SRCDIR or run in source dir)
endif

LDFLAGS += -X main.GitSummary=${GitSummary} -X main.GitBranch=${GitBranch}

.DEFAULT:
# This is the important part.
# If we're not already building a ".default" recipe, try to make <recipe>.default.
# If not, bail to avoid infinite recursion
	$(if $(filter-out %.default,$@),@$(MAKE) -C ${SRCDIR} $@.default,$(error No rule to make target '$(subst .default,,$@)'))

default: all

all.default:
# Fmt first, build later
	$(MAKE) -C ${SRCDIR} fmt
	$(MAKE) -C ${SRCDIR} build

build.default:
# Run parallel builds in sub-make
	$(MAKE) -C ${SRCDIR} ${BINS}

fmt.default:
	find ${SRCDIR} ! -path "*/vendor/*" -type f -name '*.go' -exec ${GOROOT}/bin/gofmt -l -s -w {} \;

clean.default:
	@$(foreach bin,${BINS},rm -vf ${SRCDIR}/${bin};)

installdirs.default:
	mkdir -p ${DESTDIR}${PREFIX}/{bin,}

install.default: build installdirs
	@$(foreach bin,${BINS},cp -vf ${SRCDIR}/${bin} ${DESTDIR}${PREFIX}/bin/;)

uninstall.default:
	@$(foreach bin,${BINS},rm -vf ${DESTDIR}${PREFIX}/bin/${bin};)

.PHONY: all.default build.default fmt.default clean.default installdirs.default install.default uninstall.default
# Delete default suffixes and define .go
.SUFFIXES:
.SUFFIXES: .go
