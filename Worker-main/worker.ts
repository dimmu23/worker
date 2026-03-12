import {  Worker } from "bullmq";
import { QdrantVectorStore } from "@langchain/qdrant";
import { PromptTemplate } from "@langchain/core/prompts";
import { PDFLoader } from '@langchain/community/document_loaders/fs/pdf';
import { RecursiveCharacterTextSplitter } from "langchain/text_splitter";
import { CohereEmbeddings } from "@langchain/cohere";
import axios from 'axios';
import fs from 'fs/promises';
import os from 'os';
import path from "path";
import { ChatGoogleGenerativeAI } from "@langchain/google-genai";
import { RunnableSequence } from "@langchain/core/runnables";
import redis from "./lib/redis";

// const redisConnection = {
//     url: process.env.REDIS_URL || 'redis://localhost:6379'
// }

const headingKeywords = [
  "abstract",
  "introduction",
  "related work",
  "literature review",
  "background",
  "methodology",
  "methods",
  "experiments",
  "results",
  "discussion",
  "conclusion",
  "future work",
  "references"
];

const worker = new Worker("file-upload-queue",
    async (job)=>{
        try {            
            const { paperId, pdfUrl } = job.data;
            console.log("Job received:", job.data);

            const localPath = await downloadPDF(pdfUrl);
            const loader = new PDFLoader(localPath);
            const docs = await loader.load();

            const fullText = docs.map((doc: any) => doc.pageContent).join("\n");

            const model = new ChatGoogleGenerativeAI({
                model: "gemini-2.0-flash",
                apiKey: "AIzaSyArmAmGCdWzihI5Q78TAsrN3H5T05X_aYY"
            });

            const prompt = PromptTemplate.fromTemplate(`
                You are a helpful assistant. Read the academic paper below and summarize it in a **concise and well-structured Markdown format**. Start with the actual paper title as a level 2 Markdown heading (##), followed by a one-sentence overview of the problem and goal.

                Then include the following sections:

                ### Key Innovations and Contributions  
                - Summarize the 2–4 most important contributions using clear bullet points.

                ### Methodology  
                Summarize datasets used, model/architecture details, training parameters, and evaluation setup in 1–2 short paragraphs.

                ### Results and Implications  
                Clearly state key results (with numbers if available), efficiency insights, and the broader significance of the findings.

                ### Potential Applications  
                List 2–4 real-world applications.

                Make sure the output is no longer than **300–350 words** and includes valid Markdown only. Do not include any explanation outside the markdown.

                Paper Content:
                {paper}
            `);


            const chain = RunnableSequence.from([
                prompt,  // step 1: fill the prompt
                model    // step 2: call the model with filled prompt
            ]);

            const result = await chain.invoke({
                paper: fullText
            });

            const overview = extractText(result.content);

            // console.log(content);

            const res = await axios.post("https://paperfy.vercel.app/api/saveoverview",
                {
                    paperId,
                    overview
                }
            )

            console.log(res.data);
            

            const newDocs = docs.map(doc => {
                doc.pageContent = doc.pageContent.replace(/\n[A-Z][A-Z\s\-]{2,}\n/g, match =>
                    `\n${match.trim().toLowerCase()}\n`
                );
                return doc;
            });

            const splitter = new RecursiveCharacterTextSplitter({
                chunkSize: 1000,
                chunkOverlap: 200,
                separators: [
                    "\n\n",
                    "\n",
                    ...headingKeywords.map(h => `\n${h}\n`),
                    ". ",
                    " ",
                    "",
                ]
            });
            
            const splitDocs = await splitter.splitDocuments(newDocs.map((doc, i)=>{
                doc.metadata = {...doc.metadata, page: i+1};
                return doc;
            }));

            const embeddings = new CohereEmbeddings({
                model: "embed-english-v3.0",
                apiKey: "pxHHZy29DAsfH6TybW85qOuGB7OnHDfEcfaBUy0j"
            });

            const vectorStore = await QdrantVectorStore.fromDocuments(splitDocs, embeddings, {
                url: "https://c1ced9de-55f4-4ece-9fb5-12d97bf51073.us-west-2-0.aws.cloud.qdrant.io",
                apiKey: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIn0.Qkz1tL1HMpcIgXkHc8WzP9jJZGd8xMdKt8VnpfecDKI",
                collectionName: `pdf-${paperId}`,
            });

            await vectorStore.addDocuments(splitDocs);

            console.log("Vector embeddings added");
        } catch (err) {
            console.error("Worker failed:", err);
        }  
    },
    {
        concurrency: 100,
        connection: redis
    }
)


async function downloadPDF(pdfUrl: string): Promise<string> {
    const res = await axios.get(pdfUrl, { responseType: 'arraybuffer' });
    const tempFilePath = path.join(os.tmpdir(), `temp-${Date.now()}.pdf`);
    await fs.writeFile(tempFilePath, res.data);
    return tempFilePath;
}

function extractText(content: any): string {
  if (Array.isArray(content)) {
    return content
      .map(item =>
        typeof item === "string"
          ? item
          : typeof item === "object" && "text" in item
            ? item.text
            : ""
      )
      .join("\n");
  }

  if (typeof content === "object" && content !== null) {
    if ("text" in content && typeof content.text === "string") {
      return content.text;
    }
  }

  return String(content ?? "");
}
