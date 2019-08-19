// Code generated by github.com/tcard/make.go.mock. DO NOT EDIT.

package pgqueue

import (
	context "context"
	fmt "fmt"
	runtime "runtime"

	cmp "github.com/google/go-cmp/cmp"
)

// SubscriptionDriverMocker builds mocks for type SubscriptionDriver.
//
// Its fields match the original type's methods. Set those you expect to be
// called, then call the Mock method to get a mock that implements the original
// type.
//
// If the original type was a function, it is mapped to field Func.
//
// The Describe method is a shortcut to define this struct's fields in a
// declarative manner.
type SubscriptionDriverMocker struct {
	FetchPendingDeliveries func(a0 context.Context, a1 chan<- Delivery) (r0 error)
	InsertSubscription     func() (r0 error)
	ListenForDeliveries    func(a0 context.Context) (r0 AcceptFunc, r1 error)
}

// Describe lets you describe how the methods on the resulting mock are expected
// to be called and what they will return.
//
// When you're done describing methods, call Mock to get a mock that implements
// the behavior you described.
func (m *SubscriptionDriverMocker) Describe() SubscriptionDriverMockDescriptor {
	return SubscriptionDriverMockDescriptor{m: m}
}

// A SubscriptionDriverMockDescriptor lets you describe how the methods on the resulting mock are expected
// to be called and what they will return.
//
// When you're done describing methods, call its Mock method to get a mock that
// implements the behavior you described.
type SubscriptionDriverMockDescriptor struct {
	m                                  *SubscriptionDriverMocker
	descriptors_FetchPendingDeliveries []*SubscriptionDriverFetchPendingDeliveriesMockDescriptor
	descriptors_InsertSubscription     []*SubscriptionDriverInsertSubscriptionMockDescriptor
	descriptors_ListenForDeliveries    []*SubscriptionDriverListenForDeliveriesMockDescriptor
}

// Mock returns a mock that the SubscriptionDriver interface, following the behavior
// described by the descriptor methods.
//
// It also returns a function that should be called before the test is done to
// ensure that the expected number of calls to the mock methods happened. You
// can pass a *testing.T to it, since it implements the interface it wants.
func (d SubscriptionDriverMockDescriptor) Mock() (m SubscriptionDriverMock, assert func(t interface {
	Errorf(s string, args ...interface{})
}) (ok bool)) {
	assert = d.done()
	return d.m.Mock(), assert
}

func (d SubscriptionDriverMockDescriptor) done() func(t interface {
	Errorf(s string, args ...interface{})
}) bool {
	var atAssert []func() (method string, errs []string)
	type specErrs struct {
		fileLine string
		errs     []string
	}

	if len(d.descriptors_FetchPendingDeliveries) > 0 {
		for _, desc := range d.descriptors_FetchPendingDeliveries {
			desc := desc
			calls := 0
			prev := desc.call
			desc.call = func(a0 context.Context, a1 chan<- Delivery) (r0 error) {
				calls++
				return prev(a0, a1)
			}
			atAssert = append(atAssert, func() (method string, errs []string) {
				err := desc.times(calls)
				if err != nil {
					return "FetchPendingDeliveries", []string{err.Error()}
				}
				return "", nil
			})
		}
		d.m.FetchPendingDeliveries = func(a0 context.Context, a1 chan<- Delivery) (r0 error) {
			var matching []*SubscriptionDriverFetchPendingDeliveriesMockDescriptor
			var allErrs []specErrs
			for _, desc := range d.descriptors_FetchPendingDeliveries {
				errs := desc.argValidator(a0, a1)
				if len(errs) > 0 {
					allErrs = append(allErrs, specErrs{desc.fileLine, errs})
				} else {
					matching = append(matching, desc)
				}
			}
			if len(matching) == 1 {
				return matching[0].call(a0, a1)
			}
			var args string
			for i, arg := range []interface{}{a0, a1} {
				if i != 0 {
					args += "\n\t"
				}
				args += fmt.Sprintf("%#v", arg)
			}
			if len(matching) == 0 {
				matchingErrs := ""
				for _, errs := range allErrs {
					matchingErrs += "\n\tcandidate described at " + errs.fileLine + ":\n"
					for _, err := range errs.errs {
						matchingErrs += "\n\t\t" + err
					}
				}
				panic(fmt.Errorf("no matching candidate for call to mock for SubscriptionDriver.FetchPendingDeliveries with args:\n\n\t%+v\n\nfailing candidates:\n%s", args, matchingErrs))
			}
			matchingLines := ""
			for _, m := range matching {
				matchingLines += "\n\tcandidate described at " + m.fileLine
			}
			panic(fmt.Errorf("more than one candidate for call to mock for SubscriptionDriver.FetchPendingDeliveries with args:\n\n\t%+v\n\nmatching candidates:\n%s", args, matchingLines))
		}
	} else {
		d.m.FetchPendingDeliveries = func(a0 context.Context, a1 chan<- Delivery) (r0 error) {
			panic("unexpected call to mock for SubscriptionDriver.FetchPendingDeliveries")
		}
	}
	if len(d.descriptors_InsertSubscription) > 0 {
		for _, desc := range d.descriptors_InsertSubscription {
			desc := desc
			calls := 0
			prev := desc.call
			desc.call = func() (r0 error) {
				calls++
				return prev()
			}
			atAssert = append(atAssert, func() (method string, errs []string) {
				err := desc.times(calls)
				if err != nil {
					return "InsertSubscription", []string{err.Error()}
				}
				return "", nil
			})
		}
		d.m.InsertSubscription = func() (r0 error) {
			var matching []*SubscriptionDriverInsertSubscriptionMockDescriptor
			var allErrs []specErrs
			for _, desc := range d.descriptors_InsertSubscription {
				errs := desc.argValidator()
				if len(errs) > 0 {
					allErrs = append(allErrs, specErrs{desc.fileLine, errs})
				} else {
					matching = append(matching, desc)
				}
			}
			if len(matching) == 1 {
				return matching[0].call()
			}
			var args string
			for i, arg := range []interface{}{} {
				if i != 0 {
					args += "\n\t"
				}
				args += fmt.Sprintf("%#v", arg)
			}
			if len(matching) == 0 {
				matchingErrs := ""
				for _, errs := range allErrs {
					matchingErrs += "\n\tcandidate described at " + errs.fileLine + ":\n"
					for _, err := range errs.errs {
						matchingErrs += "\n\t\t" + err
					}
				}
				panic(fmt.Errorf("no matching candidate for call to mock for SubscriptionDriver.InsertSubscription with args:\n\n\t%+v\n\nfailing candidates:\n%s", args, matchingErrs))
			}
			matchingLines := ""
			for _, m := range matching {
				matchingLines += "\n\tcandidate described at " + m.fileLine
			}
			panic(fmt.Errorf("more than one candidate for call to mock for SubscriptionDriver.InsertSubscription with args:\n\n\t%+v\n\nmatching candidates:\n%s", args, matchingLines))
		}
	} else {
		d.m.InsertSubscription = func() (r0 error) {
			panic("unexpected call to mock for SubscriptionDriver.InsertSubscription")
		}
	}
	if len(d.descriptors_ListenForDeliveries) > 0 {
		for _, desc := range d.descriptors_ListenForDeliveries {
			desc := desc
			calls := 0
			prev := desc.call
			desc.call = func(a0 context.Context) (r0 AcceptFunc, r1 error) {
				calls++
				return prev(a0)
			}
			atAssert = append(atAssert, func() (method string, errs []string) {
				err := desc.times(calls)
				if err != nil {
					return "ListenForDeliveries", []string{err.Error()}
				}
				return "", nil
			})
		}
		d.m.ListenForDeliveries = func(a0 context.Context) (r0 AcceptFunc, r1 error) {
			var matching []*SubscriptionDriverListenForDeliveriesMockDescriptor
			var allErrs []specErrs
			for _, desc := range d.descriptors_ListenForDeliveries {
				errs := desc.argValidator(a0)
				if len(errs) > 0 {
					allErrs = append(allErrs, specErrs{desc.fileLine, errs})
				} else {
					matching = append(matching, desc)
				}
			}
			if len(matching) == 1 {
				return matching[0].call(a0)
			}
			var args string
			for i, arg := range []interface{}{a0} {
				if i != 0 {
					args += "\n\t"
				}
				args += fmt.Sprintf("%#v", arg)
			}
			if len(matching) == 0 {
				matchingErrs := ""
				for _, errs := range allErrs {
					matchingErrs += "\n\tcandidate described at " + errs.fileLine + ":\n"
					for _, err := range errs.errs {
						matchingErrs += "\n\t\t" + err
					}
				}
				panic(fmt.Errorf("no matching candidate for call to mock for SubscriptionDriver.ListenForDeliveries with args:\n\n\t%+v\n\nfailing candidates:\n%s", args, matchingErrs))
			}
			matchingLines := ""
			for _, m := range matching {
				matchingLines += "\n\tcandidate described at " + m.fileLine
			}
			panic(fmt.Errorf("more than one candidate for call to mock for SubscriptionDriver.ListenForDeliveries with args:\n\n\t%+v\n\nmatching candidates:\n%s", args, matchingLines))
		}
	} else {
		d.m.ListenForDeliveries = func(a0 context.Context) (r0 AcceptFunc, r1 error) {
			panic("unexpected call to mock for SubscriptionDriver.ListenForDeliveries")
		}
	}
	return func(t interface {
		Errorf(s string, args ...interface{})
	}) bool {
		ok := true
		for _, assert := range atAssert {
			method, errs := assert()
			for _, err := range errs {
				ok = false
				t.Errorf("mock for SubscriptionDriver.%s: %s", method, err)
			}
		}
		return ok
	}
}

// FetchPendingDeliveries starts describing a way method SubscriptionDriver.FetchPendingDeliveries is expected to be called
// and what it should return.
//
// You can call it several times to describe different behaviors, each matching different parameters.
func (d SubscriptionDriverMockDescriptor) FetchPendingDeliveries() *SubscriptionDriverFetchPendingDeliveriesMockDescriptor {
	return d.newSubscriptionDriverFetchPendingDeliveriesMockDescriptor()
}

func (d SubscriptionDriverMockDescriptor) newSubscriptionDriverFetchPendingDeliveriesMockDescriptor() *SubscriptionDriverFetchPendingDeliveriesMockDescriptor {
	_, file, line, _ := runtime.Caller(2)
	return &SubscriptionDriverFetchPendingDeliveriesMockDescriptor{
		mockDesc:     d,
		times:        func(int) error { return nil },
		argValidator: func(got_a0 context.Context, got_a1 chan<- Delivery) []string { return nil },
		fileLine:     fmt.Sprintf("%s:%d", file, line),
	}
}

// SubscriptionDriverFetchPendingDeliveriesMockDescriptor is returned by SubscriptionDriverMockDescriptor.FetchPendingDeliveries and
// holds methods to describe the mock for method SubscriptionDriver.FetchPendingDeliveries.
type SubscriptionDriverFetchPendingDeliveriesMockDescriptor struct {
	mockDesc     SubscriptionDriverMockDescriptor
	times        func(int) error
	argValidator func(got_a0 context.Context, got_a1 chan<- Delivery) []string
	call         func(a0 context.Context, a1 chan<- Delivery) (r0 error)
	fileLine     string
}

// Takes lets you specify a value with which the actual value passed to
// the mocked method SubscriptionDriver.FetchPendingDeliveries as parameter #1
// will be compared.
//
// Package "github.com/google/go-cmp/cmp" is used to do the comparison. You can
// pass extra options for it.
//
// If you want to accept any value, use TakesAny.
//
// If you want more complex validation logic, use TakesMatching.
func (d *SubscriptionDriverFetchPendingDeliveriesMockDescriptor) Takes(a0 context.Context, opts ...cmp.Option) SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith1Arg {
	prev := d.argValidator
	d.argValidator = func(got_a0 context.Context, got_a1 chan<- Delivery) []string {
		errMsgs := prev(got_a0, got_a1)
		if diff := cmp.Diff(a0, got_a0, opts...); diff != "" {
			errMsgs = append(errMsgs, "parameter #1 mismatch:\n"+diff)
		}
		return errMsgs
	}
	return SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith1Arg{d}
}

// TakesAny declares that any value passed to the mocked method
// FetchPendingDeliveries as parameter #1 is expected.
func (d *SubscriptionDriverFetchPendingDeliveriesMockDescriptor) TakesAny() SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith1Arg {
	return SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith1Arg{d}
}

// TakesMatching lets you pass a function to accept or reject the actual
// value passed to the mocked method SubscriptionDriver.FetchPendingDeliveries as parameter #1.
func (d *SubscriptionDriverFetchPendingDeliveriesMockDescriptor) TakesMatching(match func(a0 context.Context) error) SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith1Arg {
	prev := d.argValidator
	d.argValidator = func(got_a0 context.Context, got_a1 chan<- Delivery) []string {
		errMsgs := prev(got_a0, got_a1)
		if err := match(got_a0); err != nil {
			errMsgs = append(errMsgs, "parameter \"a0\" custom matcher error: "+err.Error())
		}
		return errMsgs
	}
	return SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith1Arg{d}
}

// SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith1Arg is a step forward in the description of a way that the
// method SubscriptionDriver.FetchPendingDeliveries is expected to be called, with 1
// arguments specified.
//
// It has methods to describe the next argument, if there's
// any left, or the return values, if there are any, or the times it's expected
// to be called otherwise.
type SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith1Arg struct {
	methodDesc *SubscriptionDriverFetchPendingDeliveriesMockDescriptor
}

// And lets you specify a value with which the actual value passed to
// the mocked method SubscriptionDriver.FetchPendingDeliveries as parameter #2
// will be compared.
//
// Package "github.com/google/go-cmp/cmp" is used to do the comparison. You can
// pass extra options for it.
//
// If you want to accept any value, use AndAny.
//
// If you want more complex validation logic, use AndMatching.
func (d SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith1Arg) And(a1 chan<- Delivery, opts ...cmp.Option) SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith2Args {
	prev := d.methodDesc.argValidator
	d.methodDesc.argValidator = func(got_a0 context.Context, got_a1 chan<- Delivery) []string {
		errMsgs := prev(got_a0, got_a1)
		if diff := cmp.Diff(a1, got_a1, opts...); diff != "" {
			errMsgs = append(errMsgs, "parameter #2 mismatch:\n"+diff)
		}
		return errMsgs
	}
	return SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith2Args{d.methodDesc}
}

// AndAny declares that any value passed to the mocked method
// FetchPendingDeliveries as parameter #2 is expected.
func (d SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith1Arg) AndAny() SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith2Args {
	return SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith2Args{d.methodDesc}
}

// AndMatching lets you pass a function to accept or reject the actual
// value passed to the mocked method SubscriptionDriver.FetchPendingDeliveries as parameter #2.
func (d SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith1Arg) AndMatching(match func(a1 chan<- Delivery) error) SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith2Args {
	prev := d.methodDesc.argValidator
	d.methodDesc.argValidator = func(got_a0 context.Context, got_a1 chan<- Delivery) []string {
		errMsgs := prev(got_a0, got_a1)
		if err := match(got_a1); err != nil {
			errMsgs = append(errMsgs, "parameter \"a1\" custom matcher error: "+err.Error())
		}
		return errMsgs
	}
	return SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith2Args{d.methodDesc}
}

// SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith2Args is a step forward in the description of a way that the
// method SubscriptionDriver.FetchPendingDeliveries is expected to be called, with 2
// arguments specified.
//
// It has methods to describe the next argument, if there's
// any left, or the return values, if there are any, or the times it's expected
// to be called otherwise.
type SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith2Args struct {
	methodDesc *SubscriptionDriverFetchPendingDeliveriesMockDescriptor
}

// Returns lets you specify the values that the mocked method SubscriptionDriver.FetchPendingDeliveries,
// if called with values matching the expectations, will return.
func (d SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith2Args) Returns(r0 error) SubscriptionDriverFetchPendingDeliveriesMockDescriptorWithReturn {
	return d.ReturnsFrom(func(context.Context, chan<- Delivery) error {
		return r0
	})
}

// Returns lets you specify the values that the mocked method SubscriptionDriver.FetchPendingDeliveries,
// if called with values matching the expectations, will return.
//
// It passes such passed values to a function that then returns the return values.
func (d SubscriptionDriverFetchPendingDeliveriesMockDescriptorWith2Args) ReturnsFrom(f func(a0 context.Context, a1 chan<- Delivery) (r0 error)) SubscriptionDriverFetchPendingDeliveriesMockDescriptorWithReturn {
	d.methodDesc.call = f
	return SubscriptionDriverFetchPendingDeliveriesMockDescriptorWithReturn{d.methodDesc}
}

// SubscriptionDriverFetchPendingDeliveriesMockDescriptorWithReturn is a step forward in the description of a way that
// method SubscriptionDriver.FetchPendingDeliveries is to behave when called, with all expected parameters
// and the resulting values specified.
// arguments specified.
//
// It has methods to describe the times the method is expected to be called,
// or you can start another method call description, or you can call Mock to
// end the description and get the resulting mock.
type SubscriptionDriverFetchPendingDeliveriesMockDescriptorWithReturn struct {
	methodDesc *SubscriptionDriverFetchPendingDeliveriesMockDescriptor
}

// Times lets you specify a exact number of times this method is expected to be
// called.
func (d SubscriptionDriverFetchPendingDeliveriesMockDescriptorWithReturn) Times(times int) SubscriptionDriverMockDescriptor {
	return d.TimesMatching(func(got int) error {
		if got != times {
			return fmt.Errorf("expected exactly %d calls, got %d", times, got)
		}
		return nil
	})
}

// AtLeastTimes lets you specify a minimum number of times this method is expected to be
// called.
func (d SubscriptionDriverFetchPendingDeliveriesMockDescriptorWithReturn) AtLeastTimes(times int) SubscriptionDriverMockDescriptor {
	return d.TimesMatching(func(got int) error {
		if got < times {
			return fmt.Errorf("expected at least %d calls, got %d", times, got)
		}
		return nil
	})
}

// TimesMatching lets you pass a function to accept or reject the number of times
// this method has been called.
func (d SubscriptionDriverFetchPendingDeliveriesMockDescriptorWithReturn) TimesMatching(f func(times int) error) SubscriptionDriverMockDescriptor {
	d.methodDesc.times = f
	d.methodDesc.done()
	return d.methodDesc.mockDesc
}

// Mock finishes the description and produces a mock.
//
// See SubscriptionDriverMockDescriptor.Mock for details.
func (d SubscriptionDriverFetchPendingDeliveriesMockDescriptorWithReturn) Mock() (m SubscriptionDriverMock, assert func(t interface{ Errorf(string, ...interface{}) }) (ok bool)) {
	d.methodDesc.done()
	return d.methodDesc.mockDesc.Mock()
}

// FetchPendingDeliveries finishes the current description for method SubscriptionDriver.FetchPendingDeliveries and
// starts describing for method FetchPendingDeliveries.
//
// See SubscriptionDriverMockDescriptor.FetchPendingDeliveries for details.
func (d SubscriptionDriverFetchPendingDeliveriesMockDescriptorWithReturn) FetchPendingDeliveries() *SubscriptionDriverFetchPendingDeliveriesMockDescriptor {
	d.methodDesc.done()
	return d.methodDesc.mockDesc.newSubscriptionDriverFetchPendingDeliveriesMockDescriptor()
}

// InsertSubscription finishes the current description for method SubscriptionDriver.FetchPendingDeliveries and
// starts describing for method InsertSubscription.
//
// See SubscriptionDriverMockDescriptor.InsertSubscription for details.
func (d SubscriptionDriverFetchPendingDeliveriesMockDescriptorWithReturn) InsertSubscription() *SubscriptionDriverInsertSubscriptionMockDescriptor {
	d.methodDesc.done()
	return d.methodDesc.mockDesc.newSubscriptionDriverInsertSubscriptionMockDescriptor()
}

// ListenForDeliveries finishes the current description for method SubscriptionDriver.FetchPendingDeliveries and
// starts describing for method ListenForDeliveries.
//
// See SubscriptionDriverMockDescriptor.ListenForDeliveries for details.
func (d SubscriptionDriverFetchPendingDeliveriesMockDescriptorWithReturn) ListenForDeliveries() *SubscriptionDriverListenForDeliveriesMockDescriptor {
	d.methodDesc.done()
	return d.methodDesc.mockDesc.newSubscriptionDriverListenForDeliveriesMockDescriptor()
}

func (d *SubscriptionDriverFetchPendingDeliveriesMockDescriptor) done() {
	d.mockDesc.descriptors_FetchPendingDeliveries = append(d.mockDesc.descriptors_FetchPendingDeliveries, d)
}

// InsertSubscription starts describing a way method SubscriptionDriver.InsertSubscription is expected to be called
// and what it should return.
//
// You can call it several times to describe different behaviors, each matching different parameters.
func (d SubscriptionDriverMockDescriptor) InsertSubscription() *SubscriptionDriverInsertSubscriptionMockDescriptor {
	return d.newSubscriptionDriverInsertSubscriptionMockDescriptor()
}

func (d SubscriptionDriverMockDescriptor) newSubscriptionDriverInsertSubscriptionMockDescriptor() *SubscriptionDriverInsertSubscriptionMockDescriptor {
	_, file, line, _ := runtime.Caller(2)
	return &SubscriptionDriverInsertSubscriptionMockDescriptor{
		mockDesc:     d,
		times:        func(int) error { return nil },
		argValidator: func() []string { return nil },
		fileLine:     fmt.Sprintf("%s:%d", file, line),
	}
}

// SubscriptionDriverInsertSubscriptionMockDescriptor is returned by SubscriptionDriverMockDescriptor.InsertSubscription and
// holds methods to describe the mock for method SubscriptionDriver.InsertSubscription.
type SubscriptionDriverInsertSubscriptionMockDescriptor struct {
	mockDesc     SubscriptionDriverMockDescriptor
	times        func(int) error
	argValidator func() []string
	call         func() (r0 error)
	fileLine     string
}

// Returns lets you specify the values that the mocked method SubscriptionDriver.InsertSubscription,
// if called with values matching the expectations, will return.
func (d *SubscriptionDriverInsertSubscriptionMockDescriptor) Returns(r0 error) SubscriptionDriverInsertSubscriptionMockDescriptorWithReturn {
	return d.ReturnsFrom(func() error {
		return r0
	})
}

// Returns lets you specify the values that the mocked method SubscriptionDriver.InsertSubscription,
// if called with values matching the expectations, will return.
//
// It passes such passed values to a function that then returns the return values.
func (d *SubscriptionDriverInsertSubscriptionMockDescriptor) ReturnsFrom(f func() (r0 error)) SubscriptionDriverInsertSubscriptionMockDescriptorWithReturn {
	d.call = f
	return SubscriptionDriverInsertSubscriptionMockDescriptorWithReturn{d}
}

// SubscriptionDriverInsertSubscriptionMockDescriptorWithReturn is a step forward in the description of a way that
// method SubscriptionDriver.InsertSubscription is to behave when called, with all expected parameters
// and the resulting values specified.
// arguments specified.
//
// It has methods to describe the times the method is expected to be called,
// or you can start another method call description, or you can call Mock to
// end the description and get the resulting mock.
type SubscriptionDriverInsertSubscriptionMockDescriptorWithReturn struct {
	methodDesc *SubscriptionDriverInsertSubscriptionMockDescriptor
}

// Times lets you specify a exact number of times this method is expected to be
// called.
func (d SubscriptionDriverInsertSubscriptionMockDescriptorWithReturn) Times(times int) SubscriptionDriverMockDescriptor {
	return d.TimesMatching(func(got int) error {
		if got != times {
			return fmt.Errorf("expected exactly %d calls, got %d", times, got)
		}
		return nil
	})
}

// AtLeastTimes lets you specify a minimum number of times this method is expected to be
// called.
func (d SubscriptionDriverInsertSubscriptionMockDescriptorWithReturn) AtLeastTimes(times int) SubscriptionDriverMockDescriptor {
	return d.TimesMatching(func(got int) error {
		if got < times {
			return fmt.Errorf("expected at least %d calls, got %d", times, got)
		}
		return nil
	})
}

// TimesMatching lets you pass a function to accept or reject the number of times
// this method has been called.
func (d SubscriptionDriverInsertSubscriptionMockDescriptorWithReturn) TimesMatching(f func(times int) error) SubscriptionDriverMockDescriptor {
	d.methodDesc.times = f
	d.methodDesc.done()
	return d.methodDesc.mockDesc
}

// Mock finishes the description and produces a mock.
//
// See SubscriptionDriverMockDescriptor.Mock for details.
func (d SubscriptionDriverInsertSubscriptionMockDescriptorWithReturn) Mock() (m SubscriptionDriverMock, assert func(t interface{ Errorf(string, ...interface{}) }) (ok bool)) {
	d.methodDesc.done()
	return d.methodDesc.mockDesc.Mock()
}

// FetchPendingDeliveries finishes the current description for method SubscriptionDriver.InsertSubscription and
// starts describing for method FetchPendingDeliveries.
//
// See SubscriptionDriverMockDescriptor.FetchPendingDeliveries for details.
func (d SubscriptionDriverInsertSubscriptionMockDescriptorWithReturn) FetchPendingDeliveries() *SubscriptionDriverFetchPendingDeliveriesMockDescriptor {
	d.methodDesc.done()
	return d.methodDesc.mockDesc.newSubscriptionDriverFetchPendingDeliveriesMockDescriptor()
}

// InsertSubscription finishes the current description for method SubscriptionDriver.InsertSubscription and
// starts describing for method InsertSubscription.
//
// See SubscriptionDriverMockDescriptor.InsertSubscription for details.
func (d SubscriptionDriverInsertSubscriptionMockDescriptorWithReturn) InsertSubscription() *SubscriptionDriverInsertSubscriptionMockDescriptor {
	d.methodDesc.done()
	return d.methodDesc.mockDesc.newSubscriptionDriverInsertSubscriptionMockDescriptor()
}

// ListenForDeliveries finishes the current description for method SubscriptionDriver.InsertSubscription and
// starts describing for method ListenForDeliveries.
//
// See SubscriptionDriverMockDescriptor.ListenForDeliveries for details.
func (d SubscriptionDriverInsertSubscriptionMockDescriptorWithReturn) ListenForDeliveries() *SubscriptionDriverListenForDeliveriesMockDescriptor {
	d.methodDesc.done()
	return d.methodDesc.mockDesc.newSubscriptionDriverListenForDeliveriesMockDescriptor()
}

func (d *SubscriptionDriverInsertSubscriptionMockDescriptor) done() {
	d.mockDesc.descriptors_InsertSubscription = append(d.mockDesc.descriptors_InsertSubscription, d)
}

// ListenForDeliveries starts describing a way method SubscriptionDriver.ListenForDeliveries is expected to be called
// and what it should return.
//
// You can call it several times to describe different behaviors, each matching different parameters.
func (d SubscriptionDriverMockDescriptor) ListenForDeliveries() *SubscriptionDriverListenForDeliveriesMockDescriptor {
	return d.newSubscriptionDriverListenForDeliveriesMockDescriptor()
}

func (d SubscriptionDriverMockDescriptor) newSubscriptionDriverListenForDeliveriesMockDescriptor() *SubscriptionDriverListenForDeliveriesMockDescriptor {
	_, file, line, _ := runtime.Caller(2)
	return &SubscriptionDriverListenForDeliveriesMockDescriptor{
		mockDesc:     d,
		times:        func(int) error { return nil },
		argValidator: func(got_a0 context.Context) []string { return nil },
		fileLine:     fmt.Sprintf("%s:%d", file, line),
	}
}

// SubscriptionDriverListenForDeliveriesMockDescriptor is returned by SubscriptionDriverMockDescriptor.ListenForDeliveries and
// holds methods to describe the mock for method SubscriptionDriver.ListenForDeliveries.
type SubscriptionDriverListenForDeliveriesMockDescriptor struct {
	mockDesc     SubscriptionDriverMockDescriptor
	times        func(int) error
	argValidator func(got_a0 context.Context) []string
	call         func(a0 context.Context) (r0 AcceptFunc, r1 error)
	fileLine     string
}

// Takes lets you specify a value with which the actual value passed to
// the mocked method SubscriptionDriver.ListenForDeliveries as parameter #1
// will be compared.
//
// Package "github.com/google/go-cmp/cmp" is used to do the comparison. You can
// pass extra options for it.
//
// If you want to accept any value, use TakesAny.
//
// If you want more complex validation logic, use TakesMatching.
func (d *SubscriptionDriverListenForDeliveriesMockDescriptor) Takes(a0 context.Context, opts ...cmp.Option) SubscriptionDriverListenForDeliveriesMockDescriptorWith1Arg {
	prev := d.argValidator
	d.argValidator = func(got_a0 context.Context) []string {
		errMsgs := prev(got_a0)
		if diff := cmp.Diff(a0, got_a0, opts...); diff != "" {
			errMsgs = append(errMsgs, "parameter #1 mismatch:\n"+diff)
		}
		return errMsgs
	}
	return SubscriptionDriverListenForDeliveriesMockDescriptorWith1Arg{d}
}

// TakesAny declares that any value passed to the mocked method
// ListenForDeliveries as parameter #1 is expected.
func (d *SubscriptionDriverListenForDeliveriesMockDescriptor) TakesAny() SubscriptionDriverListenForDeliveriesMockDescriptorWith1Arg {
	return SubscriptionDriverListenForDeliveriesMockDescriptorWith1Arg{d}
}

// TakesMatching lets you pass a function to accept or reject the actual
// value passed to the mocked method SubscriptionDriver.ListenForDeliveries as parameter #1.
func (d *SubscriptionDriverListenForDeliveriesMockDescriptor) TakesMatching(match func(a0 context.Context) error) SubscriptionDriverListenForDeliveriesMockDescriptorWith1Arg {
	prev := d.argValidator
	d.argValidator = func(got_a0 context.Context) []string {
		errMsgs := prev(got_a0)
		if err := match(got_a0); err != nil {
			errMsgs = append(errMsgs, "parameter \"a0\" custom matcher error: "+err.Error())
		}
		return errMsgs
	}
	return SubscriptionDriverListenForDeliveriesMockDescriptorWith1Arg{d}
}

// SubscriptionDriverListenForDeliveriesMockDescriptorWith1Arg is a step forward in the description of a way that the
// method SubscriptionDriver.ListenForDeliveries is expected to be called, with 1
// arguments specified.
//
// It has methods to describe the next argument, if there's
// any left, or the return values, if there are any, or the times it's expected
// to be called otherwise.
type SubscriptionDriverListenForDeliveriesMockDescriptorWith1Arg struct {
	methodDesc *SubscriptionDriverListenForDeliveriesMockDescriptor
}

// Returns lets you specify the values that the mocked method SubscriptionDriver.ListenForDeliveries,
// if called with values matching the expectations, will return.
func (d SubscriptionDriverListenForDeliveriesMockDescriptorWith1Arg) Returns(r0 AcceptFunc, r1 error) SubscriptionDriverListenForDeliveriesMockDescriptorWithReturn {
	return d.ReturnsFrom(func(context.Context) (AcceptFunc, error) {
		return r0, r1
	})
}

// Returns lets you specify the values that the mocked method SubscriptionDriver.ListenForDeliveries,
// if called with values matching the expectations, will return.
//
// It passes such passed values to a function that then returns the return values.
func (d SubscriptionDriverListenForDeliveriesMockDescriptorWith1Arg) ReturnsFrom(f func(a0 context.Context) (r0 AcceptFunc, r1 error)) SubscriptionDriverListenForDeliveriesMockDescriptorWithReturn {
	d.methodDesc.call = f
	return SubscriptionDriverListenForDeliveriesMockDescriptorWithReturn{d.methodDesc}
}

// SubscriptionDriverListenForDeliveriesMockDescriptorWithReturn is a step forward in the description of a way that
// method SubscriptionDriver.ListenForDeliveries is to behave when called, with all expected parameters
// and the resulting values specified.
// arguments specified.
//
// It has methods to describe the times the method is expected to be called,
// or you can start another method call description, or you can call Mock to
// end the description and get the resulting mock.
type SubscriptionDriverListenForDeliveriesMockDescriptorWithReturn struct {
	methodDesc *SubscriptionDriverListenForDeliveriesMockDescriptor
}

// Times lets you specify a exact number of times this method is expected to be
// called.
func (d SubscriptionDriverListenForDeliveriesMockDescriptorWithReturn) Times(times int) SubscriptionDriverMockDescriptor {
	return d.TimesMatching(func(got int) error {
		if got != times {
			return fmt.Errorf("expected exactly %d calls, got %d", times, got)
		}
		return nil
	})
}

// AtLeastTimes lets you specify a minimum number of times this method is expected to be
// called.
func (d SubscriptionDriverListenForDeliveriesMockDescriptorWithReturn) AtLeastTimes(times int) SubscriptionDriverMockDescriptor {
	return d.TimesMatching(func(got int) error {
		if got < times {
			return fmt.Errorf("expected at least %d calls, got %d", times, got)
		}
		return nil
	})
}

// TimesMatching lets you pass a function to accept or reject the number of times
// this method has been called.
func (d SubscriptionDriverListenForDeliveriesMockDescriptorWithReturn) TimesMatching(f func(times int) error) SubscriptionDriverMockDescriptor {
	d.methodDesc.times = f
	d.methodDesc.done()
	return d.methodDesc.mockDesc
}

// Mock finishes the description and produces a mock.
//
// See SubscriptionDriverMockDescriptor.Mock for details.
func (d SubscriptionDriverListenForDeliveriesMockDescriptorWithReturn) Mock() (m SubscriptionDriverMock, assert func(t interface{ Errorf(string, ...interface{}) }) (ok bool)) {
	d.methodDesc.done()
	return d.methodDesc.mockDesc.Mock()
}

// FetchPendingDeliveries finishes the current description for method SubscriptionDriver.ListenForDeliveries and
// starts describing for method FetchPendingDeliveries.
//
// See SubscriptionDriverMockDescriptor.FetchPendingDeliveries for details.
func (d SubscriptionDriverListenForDeliveriesMockDescriptorWithReturn) FetchPendingDeliveries() *SubscriptionDriverFetchPendingDeliveriesMockDescriptor {
	d.methodDesc.done()
	return d.methodDesc.mockDesc.newSubscriptionDriverFetchPendingDeliveriesMockDescriptor()
}

// InsertSubscription finishes the current description for method SubscriptionDriver.ListenForDeliveries and
// starts describing for method InsertSubscription.
//
// See SubscriptionDriverMockDescriptor.InsertSubscription for details.
func (d SubscriptionDriverListenForDeliveriesMockDescriptorWithReturn) InsertSubscription() *SubscriptionDriverInsertSubscriptionMockDescriptor {
	d.methodDesc.done()
	return d.methodDesc.mockDesc.newSubscriptionDriverInsertSubscriptionMockDescriptor()
}

// ListenForDeliveries finishes the current description for method SubscriptionDriver.ListenForDeliveries and
// starts describing for method ListenForDeliveries.
//
// See SubscriptionDriverMockDescriptor.ListenForDeliveries for details.
func (d SubscriptionDriverListenForDeliveriesMockDescriptorWithReturn) ListenForDeliveries() *SubscriptionDriverListenForDeliveriesMockDescriptor {
	d.methodDesc.done()
	return d.methodDesc.mockDesc.newSubscriptionDriverListenForDeliveriesMockDescriptor()
}

func (d *SubscriptionDriverListenForDeliveriesMockDescriptor) done() {
	d.mockDesc.descriptors_ListenForDeliveries = append(d.mockDesc.descriptors_ListenForDeliveries, d)
}

// Mock returns a mock for SubscriptionDriver that calls the functions
// defined as struct fields in the receiver.
//
// You probably want to use Describe instead.
func (m *SubscriptionDriverMocker) Mock() SubscriptionDriverMock {
	return _makegomock_SubscriptionDriverMockFromMocker{m}
}

type _makegomock_SubscriptionDriverMockFromMocker struct {
	m *SubscriptionDriverMocker
}

func (m _makegomock_SubscriptionDriverMockFromMocker) FetchPendingDeliveries(a0 context.Context, a1 chan<- Delivery) (r0 error) {
	return m.m.FetchPendingDeliveries(a0, a1)
}

func (m _makegomock_SubscriptionDriverMockFromMocker) InsertSubscription() (r0 error) {
	return m.m.InsertSubscription()
}

func (m _makegomock_SubscriptionDriverMockFromMocker) ListenForDeliveries(a0 context.Context) (r0 AcceptFunc, r1 error) {
	return m.m.ListenForDeliveries(a0)
}

// SubscriptionDriverMock is a mock with the same underlying type as SubscriptionDriver.
//
// It is copied from the original just to avoid introducing a dependency on
// SubscriptionDriver's package.
type SubscriptionDriverMock interface {
	FetchPendingDeliveries(a0 context.Context, a1 chan<- Delivery) (r0 error)
	InsertSubscription() (r0 error)
	ListenForDeliveries(a0 context.Context) (r0 AcceptFunc, r1 error)
}