#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

#define COLUMN_TASK_SIZE 32
#define COLUMN_DATE_SIZE 32
#define sizeOfAttribute(Struct, Attribute) sizeof((Struct*)0)->Attribute
// arbitrary limit while we use the array based data structure
#define TABLE_MAX_PAGES 100


/*
 *----------------Flags & Errors---------------------
 */
typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
} ExecuteResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_STRING_TOO_LONG,
} PrepareResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT,
    STATEMENT_TOTAL,
    STATEMENT_AVERAGE
} StatementType;

/*
 *------------------------Table & Row------------------
 */

typedef struct {
    int fileDescriptor;
    uint32_t fileLength;
    void* pages[TABLE_MAX_PAGES];
} Pager;


typedef struct {
    char task[COLUMN_TASK_SIZE+1];
    float hours;
    char date[COLUMN_DATE_SIZE+1];
} Row;

typedef struct {
    Pager* pager;
    uint32_t numRows;
} Table;

typedef struct {
    StatementType type;
    Row rowToInsert;
    char selectedTask[COLUMN_TASK_SIZE+1];
} Statement;

//size of each attribute + offset (sum of previous sizes to calc starting position in memory
const uint32_t TASK_SIZE = sizeOfAttribute(Row,task);
const uint32_t HOURS_SIZE = sizeOfAttribute(Row, hours);
const uint32_t DATE_SIZE = sizeOfAttribute(Row, date);
const uint32_t TASK_OFFSET = 0;
const uint32_t HOURS_OFFSET = TASK_OFFSET + TASK_SIZE;
const uint32_t DATE_OFFSET = HOURS_OFFSET + HOURS_SIZE;
const uint32_t ROW_SIZE = TASK_SIZE + HOURS_SIZE + DATE_SIZE;

//same size as pages used in most virtual memory systems, thus OS will move pages in and out of memory as a whole
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

/*
 *------------------I/O related stuff------------------
 */
// Struct for input buffer
typedef struct {
    char* buffer;
    size_t bufferLength;
    ssize_t inputLength;
} InputBuffer;



void printRow(Row* row) {
    printf("> task: %s - hours: %.2f - date: %s\n", row->task, row->hours, row->date);
}

void printPrompt() {
    printf("void ~ ");
}

//Read input from stdin and save it to buffer
void readInput(InputBuffer* inputBuffer) {
    ssize_t bytesRead = getline(&(inputBuffer->buffer), &(inputBuffer->bufferLength), stdin);

    if (bytesRead <= 0) {
    printf("Error reading input\n");
    exit(EXIT_FAILURE);
    }

    inputBuffer->inputLength = bytesRead -1;
    inputBuffer->buffer[bytesRead-1] = 0;
}

//Delete content of input buffer, make sure to free the content before free-ing the reference to the buffer
void closeInputBuffer(InputBuffer* inputBuffer){
    free(inputBuffer->buffer);
    free(inputBuffer);
}


/*
 *----------------Serialization into storage data format---------------------
 */

void serializeRow(Row* source, void* destination) {
    //copies ID_SIZE characters from source to destionation
    strncpy(destination + TASK_OFFSET, source->task, TASK_SIZE);
    memcpy(destination + HOURS_OFFSET, &(source->hours), HOURS_SIZE);
    memcpy(destination + DATE_OFFSET, &(source->date), DATE_SIZE);
}

void deserializeRow(void* source, Row* destination) {
    memcpy(&(destination->task), source + TASK_OFFSET, TASK_SIZE);
    memcpy(&(destination->hours), source + HOURS_OFFSET, HOURS_SIZE);
    memcpy(&(destination->date), source + DATE_OFFSET, DATE_SIZE);
}

// Method to return new empty input buffer
InputBuffer* newInputBuffer() {
    InputBuffer* inputBuffer = (InputBuffer*) malloc(sizeof(InputBuffer));
    inputBuffer->buffer = NULL;
    inputBuffer->bufferLength = 0;
    inputBuffer->inputLength = 0;

    return inputBuffer;
}

void* getPage(Pager* pager, uint32_t pageNum) {
    if (pageNum > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", pageNum, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (pager->pages[pageNum] == NULL) {
        //cache miss, allocate memory and load from file
        void* page = malloc(PAGE_SIZE);
        uint32_t numPages = pager->fileLength / PAGE_SIZE;

        // We might save a partial page at the end of the file
        if (pager->fileLength % PAGE_SIZE) {
            numPages +=1;
        }

        if (pageNum <= numPages) {
            lseek(pager->fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);
            ssize_t bytesRead = read(pager->fileDescriptor, page, PAGE_SIZE);
            if (bytesRead == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }

        pager->pages[pageNum] = page;
    }
    return pager->pages[pageNum];
}

void pagerFlush(Pager* pager, uint32_t pageNum, uint32_t size) {
    if(pager->pages[pageNum] == NULL) {
        printf("Tried to flush null pages.\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);

    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytesWritten = write(pager->fileDescriptor, pager->pages[pageNum], size);
    
    if(bytesWritten == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

void closeDB(Table* table) {
    Pager* pager = table->pager;
    uint32_t numFullPages = table->numRows / ROWS_PER_PAGE;

    for (uint32_t i = 0; i < numFullPages; i++) {
        if (pager->pages[i] == NULL){
            continue;
        }
        pagerFlush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    // THere might be a partial page to write to the end of the file, this should not be needed after we swtich to a B-tree
    
    uint32_t numAdditionalRows = table->numRows % ROWS_PER_PAGE;
    if (numAdditionalRows > 0) {
        uint32_t pageNum = numFullPages;
        if (pager->pages[pageNum] != NULL) {
            pagerFlush(pager, pageNum, numAdditionalRows * ROW_SIZE);
            free(pager->pages[pageNum]);
            pager->pages[pageNum] = NULL;
        }
    }

    int result = close(pager->fileDescriptor);
    if (result == -1) {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0;i< TABLE_MAX_PAGES; i++) {
        void* page = pager->pages[i];
        if (page) {
            free(page);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
    free(table);
}

void* getRowSlot(Table* table, uint32_t rowNum) {
    uint32_t pageNum = rowNum / ROWS_PER_PAGE;
    void* page = getPage(table->pager, pageNum);
    uint32_t rowOffset = rowNum % ROWS_PER_PAGE;
    uint32_t byteOffset = rowOffset * ROW_SIZE;
    //return memory slot at start of page + offset from previous rows
    return page + byteOffset;
}

MetaCommandResult doMetaCommand(InputBuffer* inputBuffer, Table* table) {
  if (strcmp(inputBuffer->buffer, ".exit") == 0) {
      closeDB(table);
    exit(EXIT_SUCCESS);
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

PrepareResult prepareInsert(InputBuffer* inputBuffer, Statement* statement){
    statement->type = STATEMENT_INSERT;

    char* keyword = strtok(inputBuffer->buffer, " ");
    char* task = strtok(NULL, " ");
    char* hours_string = strtok(NULL, " ");

   
    if (task == NULL || hours_string == NULL){
        return PREPARE_SYNTAX_ERROR;
    }
    
    int hours = atoi(hours_string);
    if (strlen(task) > COLUMN_TASK_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    
    strcpy(statement->rowToInsert.task, task);
    statement->rowToInsert.hours = hours;
    time_t t = time(NULL);
    struct tm *tm = localtime(&t);
    strcpy(statement->rowToInsert.date, asctime(tm));

    return PREPARE_SUCCESS;
}

PrepareResult prepareTotal(InputBuffer* inputBuffer, Statement* statement){
    statement->type = STATEMENT_TOTAL;
    char* keyword = strtok(inputBuffer->buffer, " ");
    char* task = strtok(NULL, " ");

    if (task == NULL){
        return PREPARE_SYNTAX_ERROR;
    }
    strcpy(statement->selectedTask, task);
    return PREPARE_SUCCESS;
}

PrepareResult prepareAverage(InputBuffer* inputBuffer, Statement* statement){
    statement->type = STATEMENT_AVERAGE;
    char* keyword = strtok(inputBuffer->buffer, " ");
    char* task = strtok(NULL, " ");
    if (task == NULL){
        task = "*";
    }
    // If task is zero, we just take the global average
    strcpy(statement->selectedTask, task);
    return PREPARE_SUCCESS;
}

PrepareResult prepareStatement(InputBuffer* inputBuffer, Statement* statement) {
    if (strncmp(inputBuffer->buffer, "insert", 6) == 0) {
        return prepareInsert(inputBuffer, statement);        
    }
    if (strcmp(inputBuffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    if (strncmp(inputBuffer->buffer, "total", 5) == 0) {
        return prepareTotal(inputBuffer, statement);
    }
    if (strncmp(inputBuffer->buffer, "average", 7) == 0) {
        return prepareAverage(inputBuffer, statement);
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult executeInsert(Statement* statement, Table* table) {
    if (table->numRows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }

    Row* rowToInsert = &(statement->rowToInsert);
    time_t t = time(NULL);
    struct tm *tm = localtime(&t);
    serializeRow(rowToInsert, getRowSlot(table, table->numRows));
    table->numRows += 1;

    return EXECUTE_SUCCESS;
}

ExecuteResult executeSelect(Statement* statement, Table* table) {
    Row row;
    for (uint32_t i = 0; i < table->numRows; i++) {
        deserializeRow(getRowSlot(table, i), &row);
        printRow(&row);
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult executeTotal(Statement* statement, Table* table) {
    float total = 0;
    Row row;
    for (uint32_t i = 0; i< table->numRows; i++) {
        deserializeRow(getRowSlot(table, i), &row);
        if(strcmp(row.task, statement->selectedTask) == 0) {
            total += row.hours;
        }
    }
    printf("> task: %s - total time: %.2f\n",statement->selectedTask, total);
    return EXECUTE_SUCCESS;
}

ExecuteResult executeAverage(Statement* statement, Table* table) {
    float total = 0;
    uint32_t rows = 0;
    Row row;
    for (uint32_t i = 0; i< table->numRows; i++) {
        deserializeRow(getRowSlot(table, i), &row);
        if(strcmp(statement->selectedTask, "*") == 0 || strcmp(row.task, statement->selectedTask) == 0) {
            total += row.hours;
            rows += 1;
        }
    }
    if (!strcmp(statement->selectedTask, "*") == 0){
    printf("> task: %s - average time: %.2f\n",statement->selectedTask, total / rows);
    } else {
        printf("> global average: %.2f\n",total / rows);
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult executeStatement(Statement* statement, Table* table) {
    switch(statement->type) {
        case (STATEMENT_INSERT):
            return executeInsert(statement, table);
        case (STATEMENT_SELECT):
            return executeSelect(statement, table);
        case (STATEMENT_TOTAL):
            return executeTotal(statement, table);
        case (STATEMENT_AVERAGE):
            return executeAverage(statement, table);
    }
}

Pager* pagerOpen(const char* filename) {
    int fd = open(filename,
            O_RDWR | O_CREAT, // Read/write or create
            S_IWUSR | S_IRUSR); // write/read permissions

    if (fd == -1){
        printf("unable to open file.\n");
        exit(EXIT_FAILURE);
    }

        
    off_t fileLength = lseek(fd, 0, SEEK_END);

    Pager* pager = malloc(sizeof(Pager));
    pager->fileDescriptor = fd;
    pager->fileLength = fileLength;

    for (uint32_t i =0;i<TABLE_MAX_PAGES;i++){
        pager->pages[i] = NULL;
    }
    return pager;
}

Table* openDB(const char* filename) {
    Pager* pager = pagerOpen(filename);
    uint32_t numRows = pager->fileLength / ROW_SIZE;

    Table* table = malloc(sizeof(Table));
    table->pager = pager;
    table->numRows = numRows;
    
    return table;
}



// main loop, reads input, currently only recognized '.exit' as a valid command
int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("Must supply a db filename.\n");
        exit(EXIT_FAILURE);
    }

    char* filename = argv[1];
    Table* table = openDB(filename);
    InputBuffer* inputBuffer = newInputBuffer();
    while (true) {
        printPrompt();
        //Waiting for input
        readInput(inputBuffer);
        //Got input, now handle it
        //we use a leading '.' to recognize non sql commands aka meta commands
        if (inputBuffer->buffer[0] == '.') {
            switch (doMetaCommand(inputBuffer, table)) {
                case(META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '.%s'.\n", inputBuffer->buffer);
                    continue;
            }
        }
        Statement statement;
        switch (prepareStatement(inputBuffer, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_STRING_TOO_LONG):
                printf("String is too long.\n");
                continue;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax error, could not parse statement.\n");
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n", inputBuffer->buffer);
                continue;
        }

        switch (executeStatement(&statement, table)) {
            case (EXECUTE_SUCCESS):
                break;
            case (EXECUTE_TABLE_FULL):
                printf("Error: table full.\n");
                break;
        }
    }
}
