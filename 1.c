#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <jansson.h>

#define COMMON_IMPLEMENTATION
#include "common.h"

typedef struct {
    char *src;
    char *dest;
    struct {
        char *type;
        int64_t msg_id;
        char *echo;
    } body;
} EchoRequest;

typedef struct {
    char *src;
    char *dest;
    struct {
        char *type;
        int64_t msg_id;
        int64_t in_reply_to;
        char *echo;
    } body;
} EchoResponse;

// serializes the message to the spec here: https://fly.io/dist-sys/1/
EchoRequest deserialize_message(char *json)
{
    EchoRequest m = {0};

    json_t *root;
    json_error_t error;

    root = json_loads(json, 0, &error);
    m.src = strdup(json_string_value(json_object_get(root, "src")));
    m.dest = strdup(json_string_value(json_object_get(root, "dest")));
    json_t *body = json_object_get(root, "body");
    m.body.type = strdup(json_string_value(json_object_get(body, "type")));
    m.body.msg_id = (int64_t)json_number_value(json_object_get(body, "msg_id"));
    m.body.echo = strdup(json_string_value(json_object_get(body, "echo")));

    json_decref(root);

    return m;
}

EchoResponse create_response_for_request(EchoRequest *request)
{
    EchoResponse response = {0};

    response.src = request->dest;
    response.dest = request->src;
    response.body.type = "echo_ok";
    response.body.msg_id = request->body.msg_id;
    response.body.in_reply_to = request->body.msg_id;
    response.body.echo = request->body.echo;

    return response;
}

// deserializes the message to the spec here: https://fly.io/dist-sys/1/
char *serialize_message(EchoResponse *response)
{
    json_t *root = json_object();
    json_object_set_new(root, "src", json_string(response->src));
    json_object_set_new(root, "dest", json_string(response->dest));
    json_t *body = json_object();
    json_object_set_new(body, "type", json_string(response->body.type));
    json_object_set_new(body, "msg_id", json_integer(response->body.msg_id));
    json_object_set_new(body, "in_reply_to", json_integer(response->body.in_reply_to));
    json_object_set_new(body, "echo", json_string(response->body.echo));
    json_object_set_new(root, "body", body);

    char *output = json_dumps(root, JSON_COMPACT);

    json_decref(root);

    return output;
}

void release_echo_request(EchoRequest *request)
{
    free(request->src);
    free(request->dest);
    free(request->body.echo);
    free(request->body.type);
}

int main(int argc, char **argv)
{
    char *line = NULL;
    size_t len = 0;
    ssize_t nread = 0;

    while ((nread = getline(&line, &len, stdin)) != -1) {
        char *tline = trim(line);

        EchoRequest request = deserialize_message(tline);
        EchoResponse response = create_response_for_request(&request);
        char *output_json = serialize_message(&response);

        printf("%s\n", output_json);

        free(output_json);
        release_echo_request(&request);
    }

    free(line);

    return 0;
}
