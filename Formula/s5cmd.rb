# Classname should match the name of the installed package.
class S5cmd < Formula
  desc "Parallel S3 and local filesystem execution tool."
  homepage "https://github.com/peak/s5cmd"

  # Source code archive. Each tagged release will have one
  url "https://github.com/peak/s5cmd/archive/v0.6.1.tar.gz"
  sha256 "6c259da9352ae00c8b4985546f51b0f36edb58ff430196d787bea64951ad95f6"
  head "https://github.com/peak/s5mcd"

  depends_on "go" => :build

  def install
    ENV["GOPATH"] = buildpath

    bin_path = buildpath/"src/github.com/peak/s5cmd"
    # Copy all files from their current location (GOPATH root)
    # to $GOPATH/src/github.com/peak/s3hash
    bin_path.install Dir["*"]
    cd bin_path do
      # Install the compiled binary into Homebrew's `bin` - a pre-existing
      # global variable
      system "go", "build", "-o", bin/"s5cmd"
    end

    ohai "To install shell completion, run s5cmd -cmp-install"
  end

  # Homebrew requires tests.
  test do
    # "2>&1" redirects standard error to stdout.
    assert_match "s5cmd version v0.6.1", shell_output("#{bin}/s5cmd -version 2>&1", 0)
  end
end