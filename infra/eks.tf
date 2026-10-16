locals {
  # Settings passed to data jobs (helm_python_job). Database values come from the same
  # local.env_vars the backend Lambdas use; the password is passed separately as a secret.
  job_env_vars = {
    IS_LOCAL        = local.env_vars["IS_LOCAL"]
    AWS_REGION      = data.aws_region.this.region
    POSTGRES_HOST   = local.env_vars["POSTGRES_HOST"]
    POSTGRES_PORT   = local.env_vars["POSTGRES_PORT"]
    POSTGRES_NAME   = local.env_vars["POSTGRES_NAME"]
    POSTGRES_USER   = local.env_vars["POSTGRES_USER"]
    LANDING_BUCKET  = format("%s-landing-%s", var.aws_project, local.app_id)
    DATALAKE_BUCKET = format("%s-datalake-%s", var.aws_project, local.app_id)
  }
  # Settings for JupyterHub notebook servers, so the insights notebook reads the cloud lake as-is.
  notebook_env_vars = {
    IS_LOCAL        = local.job_env_vars["IS_LOCAL"]
    AWS_REGION      = local.job_env_vars["AWS_REGION"]
    DATALAKE_BUCKET = local.job_env_vars["DATALAKE_BUCKET"]
  }
  # JupyterHub admits members of the participant's LDAP group (allowed_groups below) OR these users.
  # Workshop usernames are "u" + participant id (e.g. u77a23259); the account was not in the group,
  # so the hub logged "User '...' not allowed" although its LDAP password check succeeded.
  jupyterhub_allowed_users = [format("u%s", local.app_id)]
}

resource "aws_eks_cluster" "this" {
  count    = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? 1 : 0
  name     = format("%s-%s", var.aws_project, local.app_id)
  version  = "1.36"
  role_arn = local.eks_role_arn

  access_config {
    authentication_mode                         = "API_AND_CONFIG_MAP"
    bootstrap_cluster_creator_admin_permissions = true
  }

  vpc_config {
    endpoint_public_access  = true
    endpoint_private_access = true
    subnet_ids              = local.public_subnet_ids
    security_group_ids      = data.aws_security_groups.this.ids
  }

  tags = local.app_tags
}

resource "aws_eks_node_group" "this" {
  count           = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? 1 : 0
  cluster_name    = one(aws_eks_cluster.this.*.name)
  node_group_name = format("%s-%s", var.aws_project, local.app_id)
  node_role_arn   = local.eks_role_arn
  subnet_ids      = local.public_subnet_ids
  capacity_type   = var.aws_eks_type
  instance_types  = ["t3.medium", "t3a.medium", "t2.medium"]
  disk_size       = 50

  scaling_config {
    desired_size = 1
    max_size     = 1
    min_size     = 1
  }

  update_config {
    max_unavailable = 1
  }

  tags = local.app_tags
}

resource "null_resource" "helm_chart" {
  count = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? 1 : 0

  triggers = {
    cluster_id    = one(aws_eks_node_group.this.*.id)
    source_hash   = local.helm_hash
    aws_ds_ip     = var.aws_ds_ip
    notebook_env  = jsonencode(local.notebook_env_vars) # re-install when the notebook settings change
    allowed_users = join(",", local.jupyterhub_allowed_users)
  }

  provisioner "local-exec" {
    command = <<-EOT
      if [ -z "${var.aws_ds_ip}" ] || [ "${var.aws_ds_ip}" = "null" ]; then echo "ERROR: aws_ds_ip is not set"; exit 1; fi && \
      aws eks update-kubeconfig --region ${data.aws_region.this.region} --name ${one(aws_eks_cluster.this.*.name)} || true && \
      helm repo add jupyterhub https://hub.jupyter.org/helm-chart/ && helm repo update && \
      helm upgrade --cleanup-on-fail \
        --install jupyterhub jupyterhub/jupyterhub \
        --set proxy.secretToken="${sha256(random_pet.this.id)}" \
        --set hub.config.LDAPAuthenticator.server_address="${var.aws_ds_ip}" \
        --set-string "hub.config.LDAPAuthenticator.allowed_groups[0]=CN=${local.app_id}\,OU=Users\,OU=CORP\,DC=corp\,DC=codingworkshop\,DC=net" \
%{for i, user in local.jupyterhub_allowed_users~}
        --set-string "hub.config.Authenticator.allowed_users[${i}]=${user}" \
%{endfor~}
%{for key, value in local.notebook_env_vars~}
        --set-string singleuser.extraEnv.${key}="${value}" \
%{endfor~}
        --namespace default --values ./helm/config.yaml
    EOT
  }
}

# Data jobs run as the node role, which only has the standard EKS policies. Grant least-privilege
# S3 access to the two data-lake buckets: read the landing (source) bucket, read/write the lake.
# (Pod Identity would scope this per pod, but the participant role can't create associations.)
resource "aws_iam_role_policy" "eks_data_jobs_s3" {
  count = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? 1 : 0
  name  = format("%s-eks-data-jobs-s3-%s", var.aws_project, local.app_id)
  role  = element(split("/", local.eks_role_arn), length(split("/", local.eks_role_arn)) - 1)

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadLandingBucket"
        Effect = "Allow"
        Action = ["s3:ListBucket", "s3:GetBucketLocation", "s3:GetObject"]
        Resource = [
          format("arn:%s:s3:::%s", data.aws_partition.this.partition, local.job_env_vars["LANDING_BUCKET"]),
          format("arn:%s:s3:::%s/*", data.aws_partition.this.partition, local.job_env_vars["LANDING_BUCKET"]),
        ]
      },
      {
        Sid    = "ReadWriteDataLake"
        Effect = "Allow"
        Action = ["s3:ListBucket", "s3:GetBucketLocation", "s3:CreateBucket", "s3:GetObject", "s3:PutObject",
        "s3:DeleteObject"]
        Resource = [
          format("arn:%s:s3:::%s", data.aws_partition.this.partition, local.job_env_vars["DATALAKE_BUCKET"]),
          format("arn:%s:s3:::%s/*", data.aws_partition.this.partition, local.job_env_vars["DATALAKE_BUCKET"]),
        ]
      },
    ]
  })
}

resource "null_resource" "helm_python_job" {
  for_each = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? local.data_names_python : {}

  triggers = {
    cluster_id  = one(aws_eks_node_group.this.*.id)
    source_hash = timestamp()
  }

  provisioner "local-exec" {
    # The password is passed through the environment, so it never appears in the command text.
    environment = {
      JOB_POSTGRES_PASS = local.env_vars["POSTGRES_PASS"]
    }
    # Data jobs get the same database settings as the backend Lambdas (local.env_vars),
    # plus the AWS region and the data-lake bucket names. hostNetwork=true: the nodes enforce
    # IMDSv2 with hop limit 1, so only host-network pods can fetch the node role's credentials.
    command = <<-EOT
      helm upgrade --cleanup-on-fail \
        --install ${replace(each.key, "_", "-")}-python-job ./helm/ \
        --set pythonJob.enabled=true \
        --set pythonJob.name=${replace(each.key, "_", "-")} \
        --set pythonJob.hostNetwork=true \
        --set-file pythonJob.scriptContent=${each.value.path}/${each.value.file} \
        --set-file pythonJob.requirementsContent=${each.value.path}/${each.value.reqs} \
%{for key, value in local.job_env_vars~}
        --set-string pythonJob.env.${key}="${value}" \
%{endfor~}
        --set-string pythonJob.secretEnv.POSTGRES_PASS="$JOB_POSTGRES_PASS" \
        --namespace default
    EOT
  }

  depends_on = [null_resource.helm_chart, aws_iam_role_policy.eks_data_jobs_s3]
}
