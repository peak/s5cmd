package extsort

// UniqStringChan returns a channel identical to the input but only with uniq elements
// only works on sorted inputs
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
