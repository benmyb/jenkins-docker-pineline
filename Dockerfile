FROM scratch
USER root
COPY hello /
CMD ["/hello"]
