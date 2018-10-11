FROM scratch
COPY hello /
RUN chmod a+x /hello
CMD ["/hello"]
