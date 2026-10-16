# zkrp's three images, built together from one Dockerfile:
#
#   docker buildx bake -f docker-bake.hcl           # all three, for this machine
#   docker buildx bake -f docker-bake.hcl relay     # one
#   REGISTRY=ghcr.io/you TAG=abc123 docker buildx bake -f docker-bake.hcl --push
#
# Name the file: without -f, bake also reads compose.yml and merges its
# services in as targets.
#
# One bake run shares the Dockerfile's `build` stage between relay and auth,
# so the workspace is compiled once per platform, not once per image.

variable "REGISTRY" {
  default = "ghcr.io/bradybangasser"
}

# An extra tag besides `latest`, e.g. the commit SHA in CI.
variable "TAG" {
  default = ""
}

# The commit being built (CI: github.sha); stamped into the Rust images.
variable "GIT_SHA" {
  default = ""
}

# Comma-separated, e.g. linux/amd64,linux/arm64. Empty: this machine's.
variable "PLATFORMS" {
  default = ""
}

function "tags" {
  params = [name]
  result = compact([
    "${REGISTRY}/${name}:latest",
    TAG != "" ? "${REGISTRY}/${name}:${TAG}" : "",
  ])
}

group "default" {
  targets = ["relay", "auth", "caddy"]
}

target "_common" {
  context    = "."
  dockerfile = "Dockerfile"
  platforms  = PLATFORMS != "" ? split(",", PLATFORMS) : []
  args       = { GIT_SHA = GIT_SHA }
}

target "relay" {
  inherits = ["_common"]
  target   = "relay"
  tags     = tags("zkrp-relay")
}

target "auth" {
  inherits = ["_common"]
  target   = "auth"
  tags     = tags("zkrp-auth")
}

target "caddy" {
  inherits = ["_common"]
  target   = "caddy"
  tags     = tags("zkrp-caddy")
}
