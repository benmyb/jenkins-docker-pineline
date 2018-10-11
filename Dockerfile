FROM alpine:3.7
COPY hello /
RUN chmod a+x /hello
CMD ["/hello"]
