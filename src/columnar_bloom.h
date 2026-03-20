#ifndef COLUMNAR_BLOOM_H
#define COLUMNAR_BLOOM_H

#include "postgres.h"

#include "catalog/pg_type_d.h"	/* TEXTOID, VARCHAROID, BYTEAOID, UUIDOID */

/*
 * Compute the optimal number of hash functions for a Bloom filter with
 * nbytes bytes and nrows expected elements.
 *
 * Formula: k = (m / n) * ln(2), clamped to [1, 10].
 */
extern uint8_t columnar_bloom_optimal_nhashes(uint32_t nbytes, int64_t nrows);

/*
 * Add a byte string to the filter.
 * bits[]: nbytes bytes, read-write.
 */
extern void columnar_bloom_add(uint8_t *bits, uint32_t nbytes, uint8_t nhashes,
							   const char *data, int32 len);

/*
 * Test whether a byte string is possibly present.
 * Returns false only when the value is definitely absent.
 * Returns true if possibly present (or if bloom filtering is inconclusive).
 */
extern bool columnar_bloom_test(const uint8_t *bits, uint32_t nbytes, uint8_t nhashes,
								const char *data, int32 len);

/*
 * Return true if the PostgreSQL type OID is one we build Bloom filters for.
 * Supported: TEXTOID, VARCHAROID, BYTEAOID, UUIDOID.
 *
 * NUMERICOID is excluded despite being stored as a string in Arrow, because
 * different input representations of the same value ("1" vs "1.00") would
 * cause false negatives (silently missed rows) if we tested the byte strings.
 */
extern bool columnar_bloom_oid_supported(Oid pg_type);

#endif /* COLUMNAR_BLOOM_H */
