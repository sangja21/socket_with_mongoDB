import express from "express";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { Server } from "socket.io";
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import { availableParallelism } from "node:os";
import cluster from "node:cluster";
import { createAdapter, setupPrimary } from "@socket.io/cluster-adapter";
import mongoose from "mongoose";

if (cluster.isPrimary) {
  const numCPUs = availableParallelism();
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork({
      PORT: 3000 + i,
    });
  }

  setupPrimary();
} else {
  // const db = await open({
  //   filename: "chat.db",
  //   driver: sqlite3.Database,
  // });

  // await db.exec(`
  //   CREATE TABLE IF NOT EXISTS messages (
  //     id INTEGER PRIMARY KEY AUTOINCREMENT,
  //     client_offset TEXT UNIQUE,
  //     content TEXT
  //   );
  // `);

  // MongoDB 연결 추가 (삭제한 자리에 대체)
  await mongoose.connect("mongodb://localhost:27017/chat");
  console.log("✅ MongoDB connected");

  // Message 스키마 선언
  const messageSchema = new mongoose.Schema(
    {
      client_offset: { type: String, unique: true },
      content: String,
    },
    { timestamps: true }
  );

  const Message = mongoose.model("Message", messageSchema);

  const app = express();
  const server = createServer(app);
  const io = new Server(server, {
    cors: {
      origin: "*", // 모든 도메인에서 WebSocket 허용
      methods: ["GET", "POST"],
    },
    adapter: createAdapter(), // 클러스터 어댑터 사용
    connectionStateRecovery: {}, // 단일 서버에서만 권장됨
  });

  const __dirname = dirname(fileURLToPath(import.meta.url));

  app.get("/", (req, res) => {
    res.sendFile(join(__dirname, "index.html"));
  });

  io.on("connection", async (socket) => {
    console.log(`🔗 Client connected: ${socket.id}`);

    socket.on("chat message", async (msg, clientOffset, callback) => {
      // let result;
      // try {
      //   result = await db.run(
      //     "INSERT INTO messages (content, client_offset) VALUES (?, ?)",
      //     msg,
      //     clientOffset
      //   );
      // } catch (e) {
      //   if (e.errno === 19 /* SQLITE_CONSTRAINT */) {
      //     callback();
      //   } else {
      //     // nothing to do, just let the client retry
      //   }
      //   return;
      // }
      // io.emit("chat message", msg, result.lastID);
      // callback();

      let result;
      try {
        result = await Message.create({
          content: msg,
          client_offset: clientOffset,
        });
      } catch (e) {
        if (e.code === 11000) {
          // Duplicate key
          callback();
        } else {
          console.error(e);
        }
        return;
      }
      io.emit("chat message", msg, result._id.toString());
      callback();
    });

    if (!socket.recovered) {
      try {
        // await db.each(
        //   "SELECT id, content FROM messages WHERE id > ?",
        //   [socket.handshake.auth.serverOffset || 0],
        //   (_err, row) => {
        //     socket.emit("chat message", row.content, row.id);
        //   }
        // );

        if (!socket.recovered) {
          const offset =
            socket.handshake.auth.serverOffset || "000000000000000000000000"; // ObjectId 비교용
          const messages = await Message.find({ _id: { $gt: offset } }).sort({
            _id: 1,
          });

          messages.forEach((row) => {
            socket.emit("chat message", row.content, row._id.toString());
          });
        }
      } catch (e) {
        // something went wrong
      }
    }
  });

  const port = process.env.PORT;

  server.listen(port, () => {
    console.log(`server running at http://localhost:${port}`);
  });
}
