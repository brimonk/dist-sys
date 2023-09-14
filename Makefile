SRCS=$(wildcard *.c)
PRGS=$(patsubst %.c,%,$(SRCS))
CFLAGS=-Wall -g3
LDLIBS=-lm -ljansson

all: $(PRGS)

%: %.c
	$(CC) $(CFLAGS) -o ex-$@ $< $(LDLIBS)

clean:
	rm -f $(PRGS)
