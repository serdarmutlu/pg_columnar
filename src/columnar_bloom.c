#include "postgres.h"

#include <math.h>
#include <string.h>

#include "columnar_bloom.h"

/*
 * FNV-1a 64-bit hash.
 * Simple, fast, and distributes well enough for bloom filter use.
 */
#define FNV1A_PRIME		UINT64CONST(1099511628211)
#define FNV1A_OFFSET	UINT64CONST(14695981039346656037)

static uint64
fnv1a_64(const char *data, int32 len)
{
	const uint8 *p = (const uint8 *) data;
	uint64		h = FNV1A_OFFSET;
	int32		i;

	for (i = 0; i < len; i++)
	{
		h ^= (uint64) p[i];
		h *= FNV1A_PRIME;
	}
	return h;
}

/*
 * Compute the optimal number of hash functions.
 *
 * k_opt = (m / n) * ln(2)  where m = nbytes * 8 bits, n = nrows.
 * Clamped to [1, 10] to avoid excessive hash computation.
 */
uint8_t
columnar_bloom_optimal_nhashes(uint32_t nbytes, int64_t nrows)
{
	double		k;

	if (nrows <= 0)
		return 5;				/* safe default for empty stripe */

	k = ((double) nbytes * 8.0 / (double) nrows) * 0.6931471805599453;
	if (k < 1.0)
		return 1;
	if (k > 10.0)
		return 10;
	return (uint8_t) round(k);
}

/*
 * Add a byte string to the bloom filter using double hashing
 * (Kirsch-Mitzenmacher method).
 *
 * h1 = FNV-1a(data)
 * h2 = h1 rotated left by 17 bits (linearly independent of h1)
 * bit_i = (h1 + i * h2) mod nbits
 */
void
columnar_bloom_add(uint8_t *bits, uint32_t nbytes, uint8_t nhashes,
				   const char *data, int32 len)
{
	uint32_t	nbits = nbytes * 8u;
	uint64		h1 = fnv1a_64(data, len);
	uint64		h2 = (h1 << 17) | (h1 >> (64 - 17));
	int			i;

	for (i = 0; i < (int) nhashes; i++)
	{
		uint32_t	bit = (uint32_t) ((h1 + (uint64) i * h2) % (uint64) nbits);

		bits[bit >> 3] |= (uint8_t) (1u << (bit & 7));
	}
}

/*
 * Test whether a byte string is possibly present.
 * Returns false only when the value is definitely absent.
 */
bool
columnar_bloom_test(const uint8_t *bits, uint32_t nbytes, uint8_t nhashes,
					const char *data, int32 len)
{
	uint32_t	nbits = nbytes * 8u;
	uint64		h1 = fnv1a_64(data, len);
	uint64		h2 = (h1 << 17) | (h1 >> (64 - 17));
	int			i;

	for (i = 0; i < (int) nhashes; i++)
	{
		uint32_t	bit = (uint32_t) ((h1 + (uint64) i * h2) % (uint64) nbits);

		if (!(bits[bit >> 3] & (uint8_t) (1u << (bit & 7))))
			return false;		/* bit not set → definitely absent */
	}
	return true;				/* all bits set → possibly present */
}

/*
 * Return true if the PostgreSQL type OID is one we build Bloom filters for.
 */
bool
columnar_bloom_oid_supported(Oid pg_type)
{
	return (pg_type == TEXTOID ||
			pg_type == VARCHAROID ||
			pg_type == BYTEAOID ||
			pg_type == UUIDOID);
}
