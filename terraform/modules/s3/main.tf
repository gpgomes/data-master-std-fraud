locals {
  sse_algorithm = var.kms_key_arn == null ? "AES256" : "aws:kms"
}

resource "aws_s3_bucket" "this" {
  #checkov:skip=CKV_AWS_18:Access logging exige um bucket de logs extra; os dados são sintéticos e o teto é US$ 50/mês. Reavaliar se o projeto passar a guardar dados reais.
  #checkov:skip=CKV_AWS_144:Replicação cross-region dobra armazenamento e custo; fora do orçamento.
  #checkov:skip=CKV2_AWS_62:Nenhum consumidor de eventos de bucket na arquitetura.
  for_each = var.buckets

  bucket        = "${var.name_prefix}-${each.key}"
  force_destroy = var.force_destroy
}

# ACLs desabilitadas: o dono do bucket é dono de todos os objetos.
resource "aws_s3_bucket_ownership_controls" "this" {
  for_each = var.buckets

  bucket = aws_s3_bucket.this[each.key].id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each = var.buckets

  bucket                  = aws_s3_bucket.this[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = var.buckets

  bucket = aws_s3_bucket.this[each.key].id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = var.buckets

  bucket = aws_s3_bucket.this[each.key].id

  rule {
    bucket_key_enabled = var.kms_key_arn != null

    apply_server_side_encryption_by_default {
      sse_algorithm     = local.sse_algorithm
      kms_master_key_id = var.kms_key_arn
    }
  }
}

# Versões antigas expiram: o checkpoint do streaming regrava objetos o tempo todo
# e, sem isso, o custo de armazenamento cresceria sem limite.
resource "aws_s3_bucket_lifecycle_configuration" "this" {
  for_each = var.buckets

  bucket = aws_s3_bucket.this[each.key].id

  rule {
    id     = "housekeeping"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }

    noncurrent_version_expiration {
      noncurrent_days = each.value.noncurrent_version_expiration_days
    }

    expiration {
      expired_object_delete_marker = true
    }
  }

  depends_on = [aws_s3_bucket_versioning.this]
}

# Nega qualquer acesso sem TLS.
data "aws_iam_policy_document" "tls_only" {
  for_each = var.buckets

  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.this[each.key].arn, "${aws_s3_bucket.this[each.key].arn}/*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each = var.buckets

  bucket = aws_s3_bucket.this[each.key].id
  policy = data.aws_iam_policy_document.tls_only[each.key].json

  depends_on = [aws_s3_bucket_public_access_block.this]
}
