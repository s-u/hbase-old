#define USE_RINTERNALS 1
#include <Rinternals.h>

SEXP acc_create() {
  SEXP res = allocVector(VECSXP, 2), head;
  SET_VECTOR_ELT(res, 0, head = CONS(R_NilValue, R_NilValue));
  SET_VECTOR_ELT(res, 1, head);
  return res;
}

SEXP acc_append(SEXP acc, SEXP sVal) {
  SEXP tail = VECTOR_ELT(acc, 1);
  tail = SETCDR(tail, CONS(sVal, R_NilValue));
  SET_VECTOR_ELT(acc, 1, tail);
  return sVal;
}

SEXP acc_result(SEXP acc, SEXP sFn) {
  SEXP res = CDR(VECTOR_ELT(acc, 0));
  if (sFn == R_NilValue) return res;
  res = eval(PROTECT(LCONS(sFn, res)), R_GlobalEnv);
  UNPROTECT(1);
  return res;
}

