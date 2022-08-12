package  main

func TestPlay(t *testing.T) {
	a := make([]extsort.SortType, 0, 100)
	fmt.Println(unsafe.Sizeof(a))
	a = make([]extsort.SortType, 0, 1000)
	fmt.Println(unsafe.Sizeof(a))
	a = make([]extsort.SortType, 0, 1000)
	fmt.Println(unsafe.Sizeof(a))

	t.Fatal("It's intentional, don't worry")
}