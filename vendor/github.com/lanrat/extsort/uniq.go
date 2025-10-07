package extsort

// UniqStringChan returns a channel that filters out consecutive duplicate strings from the input.
// This function assumes the input channel provides strings in sorted order and uses string equality
// to detect duplicates. It preserves the first occurrence of each unique string while filtering
// out subsequent duplicates. The function is optimized for sorted inputs where duplicates appear
// consecutively, making it suitable for post-processing sorted results from external sort operations.
//
// The returned channel will be closed when the input channel is closed.
// This function spawns a goroutine that will terminate when the input channel is closed.
func UniqStringChan(in chan string) chan string {
	out := make(chan string)
	go func() {
		var prior string
		priorSet := false
		for d := range in {
			if priorSet {
				if d == prior {
					continue
				}
			} else {
				priorSet = true
			}
			out <- d
			prior = d
		}
		close(out)
	}()
	return out
}
