import { Component, createSignal } from "solid-js";
import { Message, generateUniqueId } from "../App";

const Textbar: Component<{ username: string | null }> = ({ username }) => {
  function sendMessage() {
    if (messageContent().length > 0) {
      let message: Message = {
        username: username!!,
        messageId: generateUniqueId(),
        timestamp: Date.now(),
        content: messageContent(),
      };

      console.log("send message", message);
      // where we would WRITE an append
      // Appendable.append(indexFileName: string, message: Message)

      setMessageContent("");
    }
  }

  const [messageContent, setMessageContent] = createSignal("");

  return (
    <div class="h-14 flex w-full text-lg px-2 py-1">
      <div class="w-full bg-gray-100 rounded-md flex p-2">
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

        <button class="pl-2 font-semibold" onClick={sendMessage}>
          Send
        </button>
      </div>
    </div>
  );
};

export default Textbar;
