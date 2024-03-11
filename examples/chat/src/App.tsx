import type { Component } from "solid-js";
import { init } from "../src/appendable/appendable.min.js";
import Header from "./header.jsx";
import Channel from "./chat/channel.jsx";
import Textbar from "./chat/textbar.jsx";
import { createResource, createSignal } from "solid-js";
import UserModal, { getCookie } from "./user.js";

export function generateUniqueId(): number {
  return Number(`${Date.now()}${Math.floor(Math.random() * 1000000)}`);
}

// For a more detailed guide on the design, read DESIGN.md
export type Message = {
  username: string;
  messageId: number;
  timestamp: number;
  content: string;
};

const App: Component = () => {
  async function fetchMessageData(): Promise<Message[]> {
    // fetch for all messages
    // we would be making a db.query() call here
    return [];
  }

  const [user, setUser] = createSignal<string | null>(getCookie("user"));
  const [messages, { refetch }] = createResource(fetchMessageData);

  return (
    <main class="flex justify-center w-full md:p-4 h-screen">
      <div class="w-[80em] flex flex-col bg-white shadow-lg rounded-md">
        <Header username={user()} />
        {!user() && <UserModal setUser={setUser} />}
        <Channel messages={messages} />
        <Textbar username={user()} />
      </div>
    </main>
  );
};

export default App;
