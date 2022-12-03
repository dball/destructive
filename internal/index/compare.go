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

func E[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
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

func EA[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
	diff = E(d1, d2)
	if diff == 0 {
		diff = A(d1, d2)
	}
	return
}

func EAV[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
	diff = EA(d1, d2)
	if diff == 0 {
		diff = V(d1, d2)
	}
	return
}

func A[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
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

func AE[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
	diff = A(d1, d2)
	if diff == 0 {
		diff = E(d1, d2)
	}
	return
}

func AEV[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
	diff = AE(d1, d2)
	if diff == 0 {
		diff = V(d1, d2)
	}
	return
}

func AV[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
	diff = A(d1, d2)
	if diff == 0 {
		diff = V(d1, d2)
	}
	return
}

func AVE[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
	diff = AV(d1, d2)
	if diff == 0 {
		diff = E(d1, d2)
	}
	return
}

func V[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
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

func VA[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
	diff = V(d1, d2)
	if diff == 0 {
		diff = A(d1, d2)
	}
	return
}

func VAE[X constraints.Ordered](d1 TypedDatum[X], d2 TypedDatum[X]) (diff int) {
	diff = VA(d1, d2)
	if diff == 0 {
		diff = E(d1, d2)
	}
	return
}
