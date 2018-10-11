FROM alpine:3.7
COPY hello /
RUN chmod +x /hello
CMD ["/hello"]
