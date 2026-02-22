MODULE_big = pg_columnar
EXTENSION  = pg_columnar
DATA       = sql/pg_columnar--0.1.0.sql
REGRESS    = columnar_basic columnar_types columnar_copy

OBJS = \
	src/pg_columnar.o \
	src/columnar_tableam.o \
	src/columnar_storage.o \
	src/columnar_write_buffer.o \
	src/columnar_typemap.o \
	vendor/nanoarrow/nanoarrow.o \
	vendor/nanoarrow/nanoarrow_ipc.o \
	vendor/nanoarrow/flatcc.o

PG_CPPFLAGS = -Ivendor/nanoarrow -Ivendor/nanoarrow/nanoarrow -Ivendor/nanoarrow/flatcc -Isrc -Wno-declaration-after-statement

# Optional compression library detection
ZSTD_CFLAGS := $(shell pkg-config --cflags libzstd 2>/dev/null)
ZSTD_LIBS   := $(shell pkg-config --libs libzstd 2>/dev/null)
LZ4_CFLAGS  := $(shell pkg-config --cflags liblz4 2>/dev/null)
LZ4_LIBS    := $(shell pkg-config --libs liblz4 2>/dev/null)

ifneq ($(ZSTD_LIBS),)
  PG_CPPFLAGS += $(ZSTD_CFLAGS) -DHAVE_LIBZSTD
  SHLIB_LINK  += $(ZSTD_LIBS)
endif
ifneq ($(LZ4_LIBS),)
  PG_CPPFLAGS += $(LZ4_CFLAGS) -DHAVE_LIBLZ4
  SHLIB_LINK  += $(LZ4_LIBS)
endif

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Suppress warnings in vendored nanoarrow code
vendor/nanoarrow/nanoarrow.o: CFLAGS += -w
vendor/nanoarrow/nanoarrow_ipc.o: CFLAGS += -w
vendor/nanoarrow/flatcc.o: CFLAGS += -w
