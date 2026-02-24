#get the latest alpine image from node registry
FROM node:22.19.0-alpine AS build
LABEL stage=builder

#set the working directory
WORKDIR /user_web

#copy the package and package lock files
#from local to container work directory /user_web
# COPY package*.json /user_web/

#copy all the folder contents from local to container
COPY . /user_web/

# Build-time API config for Vite production bundle
ARG VITE_API_ADDRESS
ARG VITE_API_PROD_HOST
ARG VITE_API_PROD_PORT
ARG VITE_API_PROD_PROTOCOL
ENV VITE_API_ADDRESS=${VITE_API_ADDRESS}
ENV VITE_API_PROD_HOST=${VITE_API_PROD_HOST}
ENV VITE_API_PROD_PORT=${VITE_API_PROD_PORT}
ENV VITE_API_PROD_PROTOCOL=${VITE_API_PROD_PROTOCOL}

# fix error:0308010C:digital envelope routines
# RUN export NODE_OPTIONS=--openssl-legacy-provider

# update npm
# RUN npm install npm -g

#Run command npm install to install packages
# and create a react production build
# RUN npm set timeout=100000
# Use legacy peer deps to avoid resolver conflicts in CI/builds
RUN npm install --legacy-peer-deps && npm run build

#get the latest alpine image from nginx registry
FROM nginx:1.25.5-alpine
LABEL stage=main

# Set working directory to nginx asset directory
WORKDIR /var/www/html

# Remove default nginx static assets
RUN rm -rf ./*

#we copy the output from first stage that is our react build
#into nginx html directory where it will serve our index file
# Vite/modern setups output to 'dist'
# Vite/React builds output to dist by default
COPY --from=build /user_web/dist/ .

# Copy nginx config file
# Copy custom nginx config if present; otherwise keep default
# Remove default nginx.conf only if our file exists during build
COPY nginx.conf /etc/nginx/nginx.conf

# Copy the default nginx.conf provided by tiangolo/node-frontend
# Replace default site config
RUN rm -f /etc/nginx/conf.d/default.conf || true
COPY nginx_project.conf /etc/nginx/conf.d/default.conf

# EXPOSE 500

# Containers run nginx with global directives and daemon off
# ENTRYPOINT ["nginx", "-g", "daemon off;"]

CMD ["nginx", "-g", "daemon off;"]

# docker build -t user_web:latest .

# docker build --pull --rm -f "Dockerfile" -t user_web:latest .

# docker run -d -p 80:80 user_web:latest
