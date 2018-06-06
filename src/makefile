CC=g++
CFLAGS=-O3 -Wno-write-strings -I/opt/cpp-driver/include/ -std=c++11 -I/home/tcoffee/sparse-map/ -I/home/tcoffee/boost/include -mpopcnt
LDFLAGS = -L/opt/cpp-driver/build/ -lcassandra -L`jemalloc-config --libdir` -Wl,-rpath,`jemalloc-config --libdir` -ljemalloc `jemalloc-config --libs`
SOURCES := $(shell find . -type f -name *.c)
OBJECTS := $(SOURCES:.c=.o)
DEPS := $(OBJECTS:.o=.deps)

 
t_coffee: $(OBJECTS)
	@echo " Linking..."; $(CC) $^ -o t_coffee -lm $(LDFLAGS)
 
%.o: %.c
	@echo " CC $<"; $(CC) $(CFLAGS) -I. -MD -MF $(@:.o=.deps) -c -o $@ $<
 
clean:
	@echo " Cleaning..." 
	`find . -name *.o | xargs rm -rf {}`
