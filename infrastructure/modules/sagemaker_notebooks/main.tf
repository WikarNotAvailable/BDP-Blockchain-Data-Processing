resource "aws_sagemaker_notebook_instance" "bdp_gan" {
  name                = "bdp-gan"
  role_arn            = var.sagemaker_execution_role_arn
  instance_type       = "ml.g4dn.2xlarge"
  volume_size         = 5
  platform_identifier = "notebook-al2-v3"

  instance_metadata_service_configuration {
    minimum_instance_metadata_service_version = 2
  }
}

resource "aws_sagemaker_notebook_instance" "bdp_kmeans" {
  name                = "bdp-kmeans"
  role_arn            = var.sagemaker_execution_role_arn
  instance_type       = "ml.g4dn.4xlarge"
  volume_size         = 5
  platform_identifier = "notebook-al2-v2"

  instance_metadata_service_configuration {
    minimum_instance_metadata_service_version = 2
  }
}

resource "aws_sagemaker_notebook_instance" "bdp_isolation_forest" {
  name                = "bdp-isolation-forest"
  role_arn            = var.sagemaker_execution_role_arn
  instance_type       = "ml.m5.4xlarge"
  volume_size         = 5
  platform_identifier = "notebook-al2-v3"

  instance_metadata_service_configuration {
    minimum_instance_metadata_service_version = 2
  }
}

resource "aws_sagemaker_notebook_instance" "bdp_ml" {
  name                = "bdp-ml"
  role_arn            = var.sagemaker_execution_role_arn
  instance_type       = "ml.g4dn.xlarge"
  volume_size         = 5
  platform_identifier = "notebook-al2-v3"

  instance_metadata_service_configuration {
    minimum_instance_metadata_service_version = 2
  }
}