CC = gcc
CFLAGS = -O3 -Wno-write-strings -fPIC
LDFLAGS = -shared

TARGET  = src/PPCAS.so
SOURCES = $(shell echo src/*.c)
OBJECTS = $(SOURCES:.c=.o)

all: $(TARGET)

clean:
	rm -f $(OBJECTS) $(TARGET)
	
$(TARGET) : $(OBJECTS)
	$(CC) $(CFLAGS) $(OBJECTS) -o $@ $(LDFLAGS)