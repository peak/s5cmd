class S5cmd < Formula
  desc "Parallel S3 and local filesystem execution tool"
  homepage "https://github.com/peak/s5cmd"

  # Source code archive. Each tagged release will have one
  url "https://github.com/peak/s5cmd/archive/v0.7.0.tar.gz"
  sha256 "29e1f55c45b4b86f8e5d9f2c94bfe757bdfa7fde3a24d3646d686cbf7e830001"

  depends_on "go" => :build

  def install
    ENV["GOPATH"] = buildpath

    bin_path = buildpath/"src/github.com/peak/s5cmd"
    # Copy all files from their current location (GOPATH root)
    # to $GOPATH/src/github.com/peak/s3hash
    bin_path.install Dir["*"]
    cd bin_path do
      # Install the compiled binary into Homebrew's `bin` - a pre-exisxting
      # global variable
      system "go", "build", "-o", bin/"s5cmd"
    end

    ohai "To install shell completion, run s5cmd -cmp-install"
  end

  test do
    assert_match "s5cmd version v0.7.0", shell_output("#{bin}/s5cmd -version 2>&1", 0)
  end
end
