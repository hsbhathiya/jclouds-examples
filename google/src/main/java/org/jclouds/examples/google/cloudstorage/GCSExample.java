/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jclouds.examples.google.cloudstorage;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.InputMismatchException;
import java.util.Scanner;
import java.util.UUID;

import javax.ws.rs.core.MediaType;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.io.ByteSources;
import com.google.common.base.Charsets;
import com.google.common.io.ByteSource;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;

public class GCSExample {

   /**
    * First argument (args[0]) is your service account email address.
    * 
    * Second argument (args[1]) is a path to your service account private key
    * PEM file without a password. It is used for server-to-server interactions
    * The key is not transmitted anywhere.
    * 
    * (https://developers.google.com/console/help/new/#serviceaccounts)
    * 
    * Example: somecrypticname@developer.gserviceaccount.com
    * /home/planetnik/Work/Cloud/OSS/certificate/gcp-oss.pem
    * 
    */
   public static void main(final String[] args) throws IOException {
      String serviceAccountEmailAddress = args[0];
      String serviceAccountKey = null;
      try {
         serviceAccountKey = Files.toString(new File(args[1]), Charset.defaultCharset());
      } catch (IOException e) {
         System.err.println("Cannot open service account private key PEM file: " + args[1] + "\n" + e.getMessage());
         System.exit(1);
      }
      BlobStoreContext context = GCSExample.getContext(serviceAccountEmailAddress, serviceAccountKey);
      try {
         while (chooseOption(context))
            ;
      } finally {
         /**
          * Always close the context when you're done with it.
          */
         context.close();
      }
   }

   private static BlobStoreContext getContext(final String serviceAccountEmailAddress, final String serviceAccountKey) {
      BlobStoreContext context = ContextBuilder.newBuilder("google-cloud-storage")
            .credentials(serviceAccountEmailAddress, serviceAccountKey).buildView(BlobStoreContext.class);

      return context;
   }

   private static void createContainerExample(final BlobStore blobstore) {
      String containerName = "gcs-example-container" + UUID.randomUUID();
      blobstore.createContainerInLocation(null, containerName);
      System.out.println("Deleting the Container");
      blobstore.deleteContainer(containerName);
      System.out.println("Deleted!");
   }

   private static void createContainerPutBlobAndRetrieve(final BlobStore blobstore) throws IOException {
      String containerName = "gcs-example-container" + UUID.randomUUID();
      String blobName = "gcs-jclouds-example-blob" + UUID.randomUUID();
      blobstore.createContainerInLocation(null, containerName); // create a
      // bucket
      // Create a blob
      ByteSource payload = ByteSource.wrap("data".getBytes(Charsets.UTF_8));
      Blob blob = blobstore.blobBuilder(blobName).payload(payload).contentLength(payload.size())
            .contentType(MediaType.TEXT_PLAIN).build();

      // Put the blob in the container
      String etag = blobstore.putBlob(containerName, blob);

      // Retrieve the blob
      Blob result = blobstore.getBlob(containerName, blobName);

      // Print the result
      InputStream is = result.getPayload().openStream();
      try {
         String data = CharStreams.toString(new InputStreamReader(is, Charsets.UTF_8));
         System.out.println("Object etag is: " + etag);
         System.out.println("The retrieved payload is: " + data);
      } finally {
         System.out.println("Deleting Blob and the Container");
         blobstore.deleteContainer(containerName);
         System.out.println("Deleted!");
         is.close();
      }
   }

   public static void multipartUploadExample(final BlobStore blobstore) throws IOException {
      String containerName = "gcs-example-container" + UUID.randomUUID();
      String blobName = "gcs-jclouds-example-blob" + UUID.randomUUID();
      long MB = 1024L * 1024L;

      // create a bucket
      blobstore.createContainerInLocation(null, containerName);

      // 32MB is the minimum part size
      ByteSource sourceToUpload = buildData(33 * MB);

      Blob blob = blobstore.blobBuilder(blobName).payload(sourceToUpload).contentLength(sourceToUpload.size())
            .contentType(MediaType.TEXT_PLAIN).build();
      blobstore.putBlob(containerName, blob, PutOptions.Builder.multipart(true));
   }

   @SuppressWarnings("resource")
   public static boolean chooseOption(BlobStoreContext context) throws IOException {
      Scanner scan = new Scanner(System.in);
      System.out.println("");
      System.out.println("Google Cloud Storage examples");
      System.out.println("1. Create Container (GCS Bucket)");
      System.out.println("2. Create Container, Put and retrieve blob");
      System.out.println("3. Multipart upload ");
      System.out.println("4. Provider API ");
      System.out.println("9. Exit");
      System.out.print("Choose an option: ");
      try {
         switch (scan.nextInt()) {
            case 1:
               createContainerExample(context.getBlobStore());
               break;
            case 2:
               createContainerPutBlobAndRetrieve(context.getBlobStore());
               break;
            case 3:
               multipartUploadExample(context.getBlobStore());
               break;
            case 4:
               break;
            case 5:
               providerExample(context);
               break;
            case 9:
               return false; // This will close the context
            default:
               System.out.println("Not a valid option");
               break;
         }
      } catch (InputMismatchException e) {
         System.out.println("Not a valid option");
      }
      return true;
   }

   private static void providerExample(BlobStoreContext context) {
      System.out.println("Not implemented yet");
   }

   private static ByteSource buildData(long size) {
      byte[] array = new byte[1024];
      Arrays.fill(array, (byte) 'a');
      return ByteSources.repeatingArrayByteSource(array).slice(0, size);
   }
}
