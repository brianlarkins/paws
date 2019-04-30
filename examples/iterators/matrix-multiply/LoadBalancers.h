#ifndef _LoadBalancers_H
#define _LoadBalancers_H

void ldbal_partitioned(MatMulIterator mmiter);
void ldbal_glob_getnext(MatMulIterator mmiter);
void ldbal_frag_getnext(MatMulIterator mmiter);

void ldbal_scioto(MatMulIterator mmiter);
void ldbal_scioto_progressive(MatMulIterator mmiter);

#endif
