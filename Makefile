
CC := g++ # This is the main compiler
SRCDIR := src
HEADERDIR := include
BUILDDIR := build
TEMPDIR := temp
TARGET := bin/mainexec
DOCDIR := doc/
 
SRCEXT := cpp
SOURCES := $(shell find $(SRCDIR) -type f -name *.$(SRCEXT))
OBJECTS := $(patsubst $(SRCDIR)/%,$(BUILDDIR)/%,$(SOURCES:.$(SRCEXT)=.o))
CFLAGS := -g -std=c++11 -Wall
LIB := -pthread -lboost_filesystem -lboost_system
INC := -I include 

$(TARGET): $(OBJECTS)
	@echo " Linking..."
	@echo " $(CC) $^ -o $(TARGET) $(LIB)"; $(CC) $^ -o $(TARGET) $(LIB)

$(BUILDDIR)/%.o: $(SRCDIR)/%.$(SRCEXT) $(HEADERDIR)/%.h
	@mkdir -p $(BUILDDIR)
	@echo " $(CC) $(CFLAGS) $(INC) -c -o $@ $<"; $(CC) $(CFLAGS) $(INC) -c -o $@ $<

# documentation
doc:
	cd $(DOCDIR); doxygen

# Tests
experiments: $(OBJECTS)
	@mkdir -p $(TEMPDIR)
	$(CC) $^ $(CFLAGS) test/experiments.cpp $(INC) $(LIB) -o bin/experiments

# Spikes
ticket:
	$(CC) $(CFLAGS) spikes/ticket.cpp $(INC) $(LIB) -o bin/ticket

.PHONY: clean, all
clean:
	@echo " Cleaning..."; 
	@echo " $(RM) -r $(BUILDDIR) $(TARGET) $(TEMPDIR)"; $(RM) -r $(BUILDDIR) $(TARGET) $(TEMPDIR)