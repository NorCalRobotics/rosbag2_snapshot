/// @file payload_block_math.h
/// @author Gabriel Stewart
/// @copyright UPower (c) 2022, All rights reserved

#ifndef PAYLOAD_BLOCK_MATH_H_INCLUDED
#define PAYLOAD_BLOCK_MATH_H_INCLUDED

#define KiB_BITWIDTH    10 // 1024 bytes per KiB --> 2^10 --> 1<<10
#define MiB_BITWIDTH    (KiB_BITWIDTH + KiB_BITWIDTH)
#define GiB_BITWIDTH    (KiB_BITWIDTH + MiB_BITWIDTH)
#define TiB_BITWIDTH    (KiB_BITWIDTH + GiB_BITWIDTH)
#define PiB_BITWIDTH    (KiB_BITWIDTH + TiB_BITWIDTH)

// bitwise math operations for converting 2^arbitrary-sized blocks.
#define ARB_BLOCK_LEN(BITWIDTH)         (1 << BITWIDTH)
#define ARB_BLOCK_MASK(BITWIDTH)        (ARB_BLOCK_LEN(BITWIDTH) - 1)

#define ARB_BLOCK_REMAINDER(BITWIDTH, BYTES)     (BYTES & ARB_BLOCK_MASK(BITWIDTH))
#define ARB_FULL_BLOCK_COUNT(BITWIDTH, BYTES)    ((BYTES & ~ARB_BLOCK_MASK(BITWIDTH)) >> BITWIDTH)
#define ARB_BLOCK_COUNT_RQD(BITWIDTH, BYTES)     (ARB_FULL_BLOCK_COUNT(BITWIDTH, BYTES) + ((ARB_BLOCK_REMAINDER(BITWIDTH, BYTES) != 0) ? 1 : 0))
#define ARB_SIZE_IN_STORAGE(BITWIDTH, BYTES)     ARB_BLOCK_LEN(BITWIDTH) * ARB_BLOCK_COUNT_RQD(BITWIDTH, BYTES)

#endif

