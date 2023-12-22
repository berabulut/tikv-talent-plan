; ModuleID = 'probe4.a952e13d6143486d-cgu.0'
source_filename = "probe4.a952e13d6143486d-cgu.0"
target datalayout = "e-m:o-i64:64-i128:128-n32:64-S128"
target triple = "arm64-apple-macosx11.0.0"

@alloc_3c948dae8a598ee20ca7ddbce51d1b35 = private unnamed_addr constant <{ [84 x i8] }> <{ [84 x i8] c"/private/tmp/rust-20230901-16881-ycyewx/rustc-1.72.0-src/library/core/src/num/mod.rs" }>, align 1
@alloc_0018cee7b0e6fd14eb6b63dc9db81af3 = private unnamed_addr constant <{ ptr, [16 x i8] }> <{ ptr @alloc_3c948dae8a598ee20ca7ddbce51d1b35, [16 x i8] c"T\00\00\00\00\00\00\00w\04\00\00\05\00\00\00" }>, align 8
@str.0 = internal constant [25 x i8] c"attempt to divide by zero"

; probe4::probe
; Function Attrs: uwtable
define void @_ZN6probe45probe17h305f8efbed562787E() unnamed_addr #0 {
start:
  %0 = call i1 @llvm.expect.i1(i1 false, i1 false)
  br i1 %0, label %panic.i, label %"_ZN4core3num21_$LT$impl$u20$u32$GT$10div_euclid17h52a3ae78c7cb1039E.exit"

panic.i:                                          ; preds = %start
; call core::panicking::panic
  call void @_ZN4core9panicking5panic17heae37f5423b5bc34E(ptr align 1 @str.0, i64 25, ptr align 8 @alloc_0018cee7b0e6fd14eb6b63dc9db81af3) #3
  unreachable

"_ZN4core3num21_$LT$impl$u20$u32$GT$10div_euclid17h52a3ae78c7cb1039E.exit": ; preds = %start
  ret void
}

; Function Attrs: nocallback nofree nosync nounwind willreturn memory(none)
declare i1 @llvm.expect.i1(i1, i1) #1

; core::panicking::panic
; Function Attrs: cold noinline noreturn uwtable
declare void @_ZN4core9panicking5panic17heae37f5423b5bc34E(ptr align 1, i64, ptr align 8) unnamed_addr #2

attributes #0 = { uwtable "frame-pointer"="non-leaf" "target-cpu"="apple-m1" }
attributes #1 = { nocallback nofree nosync nounwind willreturn memory(none) }
attributes #2 = { cold noinline noreturn uwtable "frame-pointer"="non-leaf" "target-cpu"="apple-m1" }
attributes #3 = { noreturn }

!llvm.module.flags = !{!0}

!0 = !{i32 8, !"PIC Level", i32 2}
