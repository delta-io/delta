#!/usr/bin/env bash

RELATIVE_SCRIPT_PATH=$(dirname -- "${BASH_SOURCE[0]:-$0}")
WORKDIR=$(realpath "$RELATIVE_SCRIPT_PATH")
TERRAFORM_DIR="$WORKDIR"
DOCKER_DIR="$WORKDIR/docker/"
KUBERNETES_DIR="$WORKDIR/kubernetes/"

export_terraform_outputs() {
    local account_id
    local region
    local ecr_repository_name
    local metastore_endpoint
    local mysql_user
    local mysql_password
    local role_name
    account_id=$(terraform -chdir="$TERRAFORM_DIR" output account_id | tr -d '"')
    region=$(terraform -chdir="$TERRAFORM_DIR" output region | tr -d '"')
    ecr_repository_name=$(terraform -chdir="$TERRAFORM_DIR" output ecr_repository_name | tr -d '"')
    metastore_endpoint=$(terraform -chdir="$TERRAFORM_DIR" output metastore_endpoint | tr -d '"')
    mysql_user=$(terraform -chdir="$TERRAFORM_DIR" output mysql_user | tr -d '"')
    mysql_password=$(terraform -chdir="$TERRAFORM_DIR" output mysql_password | tr -d '"')
    role_name=$(terraform -chdir="$TERRAFORM_DIR" output service_account_role_name | tr -d '"')
    export ACCOUNT_ID=$account_id
    export REGION=$region
    export ECR_REPOSITORY_NAME=$ecr_repository_name
    export METASTORE_ENDPOINT=$metastore_endpoint
    export MYSQL_USER=$mysql_user
    export MYSQL_PASSWORD=$mysql_password
    export SERVICE_ACCOUNT_ROLE_NAME=$role_name
}

build_docker_image() {
    TAG=0.1
    ECR_URL="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

    local build_args=""
    if [ -n "$SPARK_VERSION" ]; then
        build_args="--build-arg SPARK_VERSION=$SPARK_VERSION"
        echo "Docker build args: $build_args."
    else
        echo "No additional docker build args."
    fi

    aws ecr get-login-password --region "${REGION}" | docker login --username AWS --password-stdin "${ECR_URL}" &&
        docker build "$DOCKER_DIR" -t "${ECR_REPOSITORY_NAME}":${TAG} $build_args &&
        docker tag "${ECR_REPOSITORY_NAME}":${TAG} "${ECR_URL}"/"${ECR_REPOSITORY_NAME}":${TAG} &&
        docker tag "${ECR_REPOSITORY_NAME}":${TAG} "${ECR_URL}"/"${ECR_REPOSITORY_NAME}":latest &&
        docker push "${ECR_URL}/${ECR_REPOSITORY_NAME}:${TAG}" &&
        docker push "${ECR_URL}/${ECR_REPOSITORY_NAME}:latest"
    local return_code=$?
    export DOCKER_IMAGE="${ECR_URL}"/"${ECR_REPOSITORY_NAME}":${TAG}
    return $return_code
}

create_kubernetes_infrastructure() {
    envsubst <"$KUBERNETES_DIR"/kubernetes.yaml | kubectl apply -f -
    local return_code=$?
    return $return_code
}

print_outputs() {
    echo "Spark docker image URI: $DOCKER_IMAGE"
}

main() {
    while [[ $# -gt 0 ]]; do
        case $1 in
        --spark-version)
            SPARK_VERSION="$2"
            shift # past argument
            shift # past value
            ;;
        -* | --*)
            echo "Unknown option $1"
            exit 1
            ;;
        *)
            shift # past argument
            ;;
        esac
    done

    if ! export_terraform_outputs; then
        echo "[ERROR] Failed to extract variables."
        exit 1
    fi

    if ! build_docker_image; then
        echo "[ERROR] Failed to build docker image."
        exit 1
    fi

    if ! create_kubernetes_infrastructure; then
        echo "[ERROR] Failed to create kubernetes infrastructure."
        exit 1
    fi
    print_outputs
}

main "$@"
