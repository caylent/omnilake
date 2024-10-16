# Image is passed as a build arg by the framework
ARG IMAGE
FROM $IMAGE

RUN poetry config virtualenvs.create false

ADD . ${LAMBDA_TASK_ROOT}/omnilake
RUN rm -rf /var/task/omnilake/.venv

RUN cd ${LAMBDA_TASK_ROOT}/omnilake && poetry install --no-dev
