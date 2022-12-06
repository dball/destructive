package index

import "golang.org/x/exp/constraints"

type Lesser[X constraints.Ordered] func(d1 TypedDatum[X], d2 TypedDatum[X]) bool

func LessEAV[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (less bool) {
	switch {
	case d1.E < d2.E:
		less = true
	case d1.E > d2.E:
		less = false
	case d1.A < d2.A:
		less = true
	case d1.A > d2.A:
		less = false
	default:
		less = d1.V < d2.V
	}
	return
}

func LessAVE[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (less bool) {
	switch {
	case d1.A < d2.A:
		less = true
	case d1.A > d2.A:
		less = false
	case d1.V < d2.V:
		less = true
	case d1.V > d2.V:
		less = false
	default:
		less = d1.E < d2.E
	}
	return
}

func LessAEV[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (less bool) {
	switch {
	case d1.A < d2.A:
		less = true
	case d1.A > d2.A:
		less = false
	case d1.E < d2.E:
		less = true
	case d1.E > d2.E:
		less = false
	default:
		less = d1.V < d2.V
	}
	return
}

func LessVAE[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (less bool) {
	switch {
	case d1.V < d2.V:
		less = true
	case d1.V > d2.V:
		less = false
	case d1.A < d2.A:
		less = true
	case d1.A > d2.A:
		less = false
	default:
		less = d1.E < d2.E
	}
	return
}

type Comparer[X constraints.Ordered] func(d1 TypedDatum[X], d2 TypedDatum[X]) int

func CompareE[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
	switch {
	case d1.E < d2.E:
		diff = -1
	case d1.E > d2.E:
		diff = 1
	default:
		diff = 0
	}
	return
}

func CompareEA[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
	diff = CompareE(d1, d2)
	if diff == 0 {
		diff = CompareA(d1, d2)
	}
	return
}

func CompareEAV[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
	diff = CompareEA(d1, d2)
	if diff == 0 {
		diff = CompareV(d1, d2)
	}
	return
}

func CompareA[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
	switch {
	case d1.A < d2.A:
		diff = -1
	case d1.A > d2.A:
		diff = 1
	default:
		diff = 0
	}
	return
}

func CompareAE[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
	diff = CompareA(d1, d2)
	if diff == 0 {
		diff = CompareE(d1, d2)
	}
	return
}

func CompareAEV[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
	diff = CompareAE(d1, d2)
	if diff == 0 {
		diff = CompareV(d1, d2)
	}
	return
}

func CompareAV[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
	diff = CompareA(d1, d2)
	if diff == 0 {
		diff = CompareV(d1, d2)
	}
	return
}

func CompareAVE[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
	diff = CompareAV(d1, d2)
	if diff == 0 {
		diff = CompareE(d1, d2)
	}
	return
}

func CompareV[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
	switch {
	case d1.V < d2.V:
		diff = -1
	case d1.V > d2.V:
		diff = 1
	default:
		diff = 0
	}
	return
}

func CompareVA[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
	diff = CompareV(d1, d2)
	if diff == 0 {
		diff = CompareA(d1, d2)
	}
	return
}

func CompareVAE[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
	diff = CompareVA(d1, d2)
	if diff == 0 {
		diff = CompareE(d1, d2)
	}
	return
}
