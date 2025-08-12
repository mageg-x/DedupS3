import { S3Client, ListBucketsCommand, CreateBucketCommand, DeleteBucketCommand } from '@aws-sdk/client-s3'

let s3Client = null

export const initializeS3Client = (credentials) => {
  const { accessKeyId, secretAccessKey, region = 'us-east-1', endpoint } = credentials

  const config = {region, credentials: {
      accessKeyId,
      secretAccessKey,
    },
  };

  if (endpoint) {
    config.endpoint = endpoint;
    config.forcePathStyle = true;
  }

  s3Client = new S3Client(config);
  return s3Client;
};

export const getS3Client = () => {
  if (!s3Client) {
    const credentials = JSON.parse(
      localStorage.getItem("s3-credentials") || "{}"
    );
    if (credentials.accessKeyId) {
      initializeS3Client(credentials);
    }
  }
  return s3Client;
};

export const testConnection = async (credentials) => {
  try {
    const testClient = new S3Client({
      region: credentials.region || "us-east-1",
      credentials: {
        accessKeyId: credentials.accessKeyId,
        secretAccessKey: credentials.secretAccessKey,
      },
      ...(credentials.endpoint && {
        endpoint: credentials.endpoint,
        forcePathStyle: true,
      }),
    });

    await testClient.send(new ListBucketsCommand({}));
    return true;
  } catch (error) {
    console.error("Connection test failed:", error);
    return false;
  }
};
