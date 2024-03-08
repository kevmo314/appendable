import { Component, createSignal } from "solid-js";
import { Message, generateUniqueId } from "../App";

const Textbar: Component = () => {
  function sendMessage() {
    if (messageContent().length > 0) {
      console.log("send message", messageContent());

      let message: Message = {
        userId: 0,
        messageId: generateUniqueId(),
        timestamp: Date.now(),
        content: messageContent(),
      };

      // where we would WRITE an append
      // Appendable.append(indexFileName: string, message: Message)

      setMessageContent("");
    }
  }

  const [messageContent, setMessageContent] = createSignal("");

  return (
    <div class="h-12 flex w-full px-2 py-1">
      <div class="w-full bg-gray-100 rounded-md flex px-2">
        <input
          class="flex-1 outline-none bg-gray-100"
          value={messageContent()}
          onInput={(ev) => {
            setMessageContent(ev.target.value);
          }}
          onKeyDown={(ev) => {
            if (ev.key === "Enter") {
              ev.preventDefault();
              sendMessage();
            }
          }}
        />

        <button class="pl-2" onClick={sendMessage}>
          Send
        </button>
      </div>
    </div>
  );
};

export default Textbar;
