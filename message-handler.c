#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include <jansson.h>

#define COMMON_IMPLEMENTATION
#include "common.h"

typedef enum {
    MESSAGE_TYPE_NONE,
    MESSAGE_TYPE_ERROR,
    MESSAGE_TYPE_INIT,
    MESSAGE_TYPE_INIT_OK,
    MESSAGE_TYPE_ECHO,
    MESSAGE_TYPE_ECHO_OK,
    MESSAGE_TYPE_GENERATE,
    MESSAGE_TYPE_GENERATE_OK,
    MESSAGE_TYPE_TOTAL,
} MESSAGE_TYPE;

char *MESSAGE_TYPE_TABLE[] = {
    NULL,
    "error",
    "init",
    "init_ok",
    "echo",
    "echo_ok",
    "generate",
    "generate_ok",
    NULL
};

typedef enum {
    ERROR_TIMEOUT = 0,
    ERROR_NODENOTFOUND = 1,
    ERROR_NOTSUPPORTED = 10,
    ERROR_TEMPORARILYUNAVAILABLE = 11,
    ERROR_MALFORMEDREQUEST = 12,
    ERROR_CRASH = 13,
    ERROR_ABORT = 14,
    ERROR_KEYDOESNOTEXIST = 20,
    ERROR_KEYALREADYEXISTS = 21,
    ERROR_PRECONDITIONFAILED = 22,
    ERROR_TXNCONFLICT = 30,
} ERRORS;

typedef struct {
    char *src;
    char *dest;

    MESSAGE_TYPE type;
    int64_t id;
    int64_t msg_id;
    int64_t in_reply_to;

    union {
        struct {
            char *node_id;
            char **node_ids; // pointer list (last element is NULL, be careful)
        } init;

        struct {
        } init_ok;

        struct {
            int64_t code;
        } error;

        // KEEP THE LAYOUT OF THE ECHO AND ECHO_OK STRUCTURES THE SAME
        struct {
            char *echo;
        } echo;

        struct {
            char *echo;
        } echo_ok;

        struct {
        } generate;

        struct {
            char *id;
        } generate_ok;
    } fields;
} SystemMessage;

static char *node_id = NULL;
static int64_t msg_id = 0;

static int64_t generate_id = 0;

MESSAGE_TYPE message_type_from_str(char *s)
{
    for (size_t i = 0; i < ARRSIZE(MESSAGE_TYPE_TABLE); i++) {
        if (MESSAGE_TYPE_TABLE[i] && streq(s, MESSAGE_TYPE_TABLE[i])) {
            return i;
        }
    }
    return MESSAGE_TYPE_NONE;
}

char *str_from_message_type(MESSAGE_TYPE type)
{
    return MESSAGE_TYPE_TABLE[type];
}

int64_t next_message_id()
{
    return ++msg_id;
}

// Parses the input message into a SystemMessage, and returns it. NULL on error. (We don't error).
SystemMessage *deserialize(char *json)
{
    SystemMessage *m = calloc(1, sizeof(*m));

    json_t *root, *body;
    json_error_t error;

    root = json_loads(json, 0, &error);
    body = json_object_get(root, "body");

    m->src = strdup(json_string_value(json_object_get(root, "src")));
    m->dest = strdup(json_string_value(json_object_get(root, "dest")));
    m->type = message_type_from_str((char *)json_string_value(json_object_get(body, "type")));
    m->id = (int64_t)json_number_value(json_object_get(body, "id"));
    m->msg_id = (int64_t)json_number_value(json_object_get(body, "msg_id"));
    m->in_reply_to = (int64_t)json_number_value(json_object_get(body, "in_reply_to"));

    switch (m->type) {
        case MESSAGE_TYPE_INIT: {
            m->fields.init.node_id = strdup(json_string_value(json_object_get(body, "node_id")));

            json_t *arr = json_object_get(body, "node_id");
            m->fields.init.node_ids = calloc(json_array_size(arr) + 1, sizeof(*m->fields.init.node_ids));

            size_t index;
            json_t *value;
            json_array_foreach(arr, index, value) {
                m->fields.init.node_ids[index] = strdup(json_string_value(value));
            }
            break;
        }
        case MESSAGE_TYPE_INIT_OK:
            break;

        case MESSAGE_TYPE_ECHO:
            m->fields.echo.echo = strdup(json_string_value(json_object_get(body, "echo")));
            break;
        case MESSAGE_TYPE_ECHO_OK:
            m->fields.echo_ok.echo = strdup(json_string_value(json_object_get(body, "echo")));
            break;

        case MESSAGE_TYPE_GENERATE:
            break;
        case MESSAGE_TYPE_GENERATE_OK:
            break;

        default:
            fprintf(stderr, "Message type: '%s' (%d) not handled!\n", MESSAGE_TYPE_TABLE[m->type], m->type);
            assert(0);
    }

    json_decref(root);

    return m;
}

SystemMessage *create_response(SystemMessage *request)
{
    SystemMessage *response = calloc(1, sizeof(*response));

    response->type = request->type;

    char tmp[128] = {0};

    switch (response->type) {
        case MESSAGE_TYPE_INIT:
            response->type = MESSAGE_TYPE_INIT_OK;
            node_id = strdup(request->fields.init.node_id);
            break;
        case MESSAGE_TYPE_INIT_OK:
            break;

        case MESSAGE_TYPE_ECHO:
            response->type = MESSAGE_TYPE_ECHO_OK;
            response->fields.echo_ok.echo = strdup(request->fields.echo.echo);
            break;
        case MESSAGE_TYPE_ECHO_OK:
            break;

        case MESSAGE_TYPE_GENERATE:
            response->type = MESSAGE_TYPE_GENERATE_OK;
            snprintf(tmp, sizeof tmp, "%s.%ld", node_id, ++generate_id);
            response->fields.generate_ok.id = strdup(tmp);
            break;
        case MESSAGE_TYPE_GENERATE_OK:
            break;

        default:
            fprintf(stderr, "Message type: '%s' (%d) not handled!\n",
                MESSAGE_TYPE_TABLE[response->type], response->type);
            assert(0);
    }

    response->src = strdup(node_id);
    response->dest = strdup(request->src);

    response->id = request->id;
    response->msg_id = next_message_id();
    response->in_reply_to = request->msg_id;

    return response;
}

char *serialize(SystemMessage *m)
{
    json_t *root = json_object();
    json_object_set_new(root, "src", json_string(m->src));
    json_object_set_new(root, "dest", json_string(m->dest));
    json_object_set_new(root, "id", json_integer(m->id));

    json_t *body = json_object();
    json_object_set_new(body, "type", json_string(str_from_message_type(m->type)));
    json_object_set_new(body, "in_reply_to", json_integer(m->in_reply_to));

    if (m->type != MESSAGE_TYPE_INIT && m->type != MESSAGE_TYPE_INIT_OK)
        json_object_set_new(body, "msg_id", json_integer(m->msg_id));

    switch (m->type) {
        case MESSAGE_TYPE_INIT:
            // TODO
            assert(0);
            break;
        case MESSAGE_TYPE_INIT_OK:
            break;

        case MESSAGE_TYPE_ECHO:
            json_object_set_new(body, "echo", json_string(m->fields.echo.echo));
            break;
        case MESSAGE_TYPE_ECHO_OK:
            json_object_set_new(body, "echo", json_string(m->fields.echo_ok.echo));
            break;

        case MESSAGE_TYPE_GENERATE:
            break;
        case MESSAGE_TYPE_GENERATE_OK:
            json_object_set_new(body, "id", json_string(m->fields.generate_ok.id));
            break;

        default:
            fprintf(stderr, "Message type: '%s' (%d) not handled!\n",
                MESSAGE_TYPE_TABLE[m->type], m->type);
            assert(0);
    }


    json_object_set_new(root, "body", body);

    char *output = json_dumps(root, JSON_COMPACT);

    json_decref(root);

    return output;
}

void free_message(SystemMessage *m)
{
    free(m->src);
    free(m->dest);

    switch (m->type) {
        case MESSAGE_TYPE_INIT:
            free(m->fields.init.node_id);
            for (size_t i = 0; m->fields.init.node_ids[i]; i++)
                free(m->fields.init.node_ids[i]);
            break;
        case MESSAGE_TYPE_INIT_OK:
            break;

        case MESSAGE_TYPE_ECHO_OK:
            free(m->fields.echo_ok.echo);
            break;
        case MESSAGE_TYPE_ECHO:
            free(m->fields.echo.echo);
            break;

        case MESSAGE_TYPE_GENERATE_OK:
            free(m->fields.generate_ok.id);
            break;
        case MESSAGE_TYPE_GENERATE:
            break;

        default:
            fprintf(stderr, "Message type: '%s' (%d) not handled!\n", MESSAGE_TYPE_TABLE[m->type], m->type);
            assert(0);
    }

    free(m);
}

int main(int argc, char **argv)
{
    char *line = NULL;
    size_t len = 0;
    ssize_t nread = 0;

    while ((nread = getline(&line, &len, stdin)) != -1) {
        char *tline = trim(line);

        fprintf(stderr, "I: %s\n", tline);

        SystemMessage *request = deserialize(tline);
        SystemMessage *response = create_response(request);
        char *json = serialize(response);

        fprintf(stderr, "O: %s\n", json);
        printf("%s\n", json);

        fflush(stdout);
        fflush(stderr);

        free_message(request);
        free_message(response);
        free(json);
    }

    free(line);
    free(node_id);

    return 0;
}
