# Least privilege por camada: cada job recebe só as policies das camadas que
# lê/escreve (ex: bronze_to_silver = read[bronze] + write[silver]). As roles que
# consomem estas policies (EMR, MWAA) chegam com os módulos de compute.
# Nenhum statement Allow usa "*" em action ou resource.

data "aws_iam_policy_document" "read" {
  for_each = var.datalake_bucket_arns

  statement {
    sid       = "ListBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [each.value]
  }

  statement {
    sid       = "ReadObjects"
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${each.value}/*"]
  }
}

data "aws_iam_policy_document" "write" {
  for_each = var.datalake_bucket_arns

  statement {
    sid       = "ListBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"]
    resources = [each.value]
  }

  statement {
    sid = "ReadWriteObjects"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${each.value}/*"]
  }
}

resource "aws_iam_policy" "read" {
  for_each = var.datalake_bucket_arns

  name        = "${var.name_prefix}-datalake-${each.key}-read"
  description = "Leitura do bucket ${each.key} do data lake."
  policy      = data.aws_iam_policy_document.read[each.key].json
}

resource "aws_iam_policy" "write" {
  for_each = var.datalake_bucket_arns

  name        = "${var.name_prefix}-datalake-${each.key}-write"
  description = "Leitura e escrita do bucket ${each.key} do data lake."
  policy      = data.aws_iam_policy_document.write[each.key].json
}
