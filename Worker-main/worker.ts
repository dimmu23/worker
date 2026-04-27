import "dotenv/config";
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
import { redisConnection } from "./lib/redis";

const apiBaseUrl = process.env.API_BASE_URL || "http://localhost:3000";
const downloadTimeoutMs = Number(process.env.PDF_DOWNLOAD_TIMEOUT_MS || 30000);
const downloadMaxAttempts = Number(process.env.PDF_DOWNLOAD_RETRIES || 3);

// const redisConnection = {
//     host: process.env.REDIS_HOST || 'localhost',
//     port: Number(process.env.REDIS_PORT) || 6379
// }

// const redisConnection = new IORedis("redis://default:BBEFuBpgdRcjapOtuuPsKVaihyxEZewS@turntable.proxy.rlwy.net:33985",{
//     maxRetriesPerRequest: null
// });

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

            console.log(`[${paperId}] Downloading PDF`);
            const localPath = await downloadPDF(pdfUrl);

            console.log(`[${paperId}] Loading PDF content`);
            const loader = new PDFLoader(localPath);
            const docs = await loader.load();

            const fullText = docs.map((doc) => doc.pageContent).join("\n");

            const model = new ChatGoogleGenerativeAI({
                model: "gemini-2.5-flash",
                apiKey: process.env.GOOGLE_API_KEY
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

            console.log(`[${paperId}] Generating overview with Gemini`);
            let result;
            try {
                result = await chain.invoke({
                    paper: fullText
                });
            } catch (err) {
                console.error(`[${paperId}] Failed to generate overview`, err);
                throw err;
            }

            const overview = extractText(result.content);

            console.log(`[${paperId}] Saving overview to main app`);

            const res = await axios.post(`${apiBaseUrl}/api/saveoverview`,
                {
                    paperId,
                    overview
                }
            )

            console.log(`[${paperId}] Overview saved`, res.status);
            
            console.log(`[${paperId}] Splitting document for embeddings`);
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
                apiKey: process.env.COHERE_API_KEY
            });

            console.log(`[${paperId}] Writing embeddings to Qdrant`);
            const vectorStore = await QdrantVectorStore.fromDocuments(splitDocs, embeddings, {
                url: process.env.QDRANT_URL!,
                apiKey: process.env.QDRANT_API_KEY!,
                collectionName: `pdf-${paperId}`,
            });

            await vectorStore.addDocuments(splitDocs);

            console.log("Vector embeddings added");
        } catch (err) {
            console.error("Worker failed:", err);
            throw err;
        }  
    },
    {
        concurrency: 100,
        connection: redisConnection
    }
)

worker.on("failed", (job, err) => {
    console.error(`Job ${job?.id ?? "unknown"} failed for paper ${job?.data?.paperId ?? "unknown"}:`, err.message);
});


async function downloadPDF(pdfUrl: string): Promise<string> {
    let lastError: unknown;

    for (let attempt = 1; attempt <= downloadMaxAttempts; attempt++) {
        try {
            console.log(`Downloading PDF attempt ${attempt}/${downloadMaxAttempts}`);

            const res = await axios.get(pdfUrl, {
                responseType: 'arraybuffer',
                timeout: downloadTimeoutMs,
                maxRedirects: 5,
                headers: {
                    Accept: 'application/pdf,application/octet-stream,*/*',
                },
            });

            const tempFilePath = path.join(os.tmpdir(), `temp-${Date.now()}.pdf`);
            await fs.writeFile(tempFilePath, res.data);
            return tempFilePath;
        } catch (err: any) {
            lastError = err;
            const code = err?.code || "UNKNOWN";
            const message = err?.message || "Unknown download error";
            console.error(`PDF download attempt ${attempt} failed [${code}]: ${message}`);

            if (attempt < downloadMaxAttempts) {
                const delayMs = attempt * 2000;
                await new Promise((resolve) => setTimeout(resolve, delayMs));
            }
        }
    }

    throw lastError;
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
