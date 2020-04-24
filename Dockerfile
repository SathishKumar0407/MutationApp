#Specify the container Image
FROM python:3.7

# Make port 80 available to the world outside this container
EXPOSE 80
EXPOSE 433
EXPOSE 8080
EXPOSE 6002

ENV HTTP_PROXY=http://webproxygo.fpl.com:8080
ENV HTTPS_PROXY=http://webproxygo.fpl.com:8080
ENV http_proxy=http://webproxygo.fpl.com:8080
ENV https_proxy=http://webproxygo.fpl.com:8080

#We are copying window setup files for .Net to the container
ADD . /adat_mutation

WORKDIR /adat_mutation

RUN pip install -r requirements.txt

CMD [ "python", "main.py" ]
