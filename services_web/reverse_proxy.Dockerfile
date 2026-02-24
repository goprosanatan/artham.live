FROM nginx:1.26.0-alpine
LABEL stage=main

RUN apk add --no-cache openssl
RUN mkdir -p /__logs/__nginx

RUN rm -f /etc/nginx/nginx.conf
COPY services_web/nginx.conf /etc/nginx/nginx.conf
RUN rm -f /etc/nginx/conf.d/default.conf
COPY services_web/nginx_project.conf /etc/nginx/conf.d/default.conf
