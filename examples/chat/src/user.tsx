import { Component, Setter, createUniqueId, onMount } from "solid-js";
import {
  adjectives,
  uniqueNamesGenerator,
  animals,
} from "unique-names-generator";

export function setCookie(name: string, value: string, days: number) {
  let expires = "";
  if (days) {
    const date = new Date();
    date.setTime(date.getTime() + days * 24 * 60 * 60 * 1000);
    expires = "; expires=" + date.toUTCString();
  }
  document.cookie = name + "=" + (value || "") + expires + "; path=/";
}

export function getCookie(name: string) {
  let nameEQ = name + "=";
  let ca = document.cookie.split(";");
  for (let i = 0; i < ca.length; i++) {
    let c = ca[i];
    while (c.charAt(0) == " ") c = c.substring(1, c.length);
    if (c.indexOf(nameEQ) == 0) return c.substring(nameEQ.length, c.length);
  }
  return null;
}

export function eraseCookie(name: string) {
  document.cookie = name + "=; Path=/; Expires=Thu, 01 Jan 1970 00:00:01 GMT;";
}

const UserModal: Component<{ setUser: Setter<string | null> }> = ({
  setUser,
}) => {
  const username = uniqueNamesGenerator({
    dictionaries: [adjectives, animals],
    separator: "-",
    seed: createUniqueId(),
  });

  function handleSetUserAndCookie() {
    setCookie("user", username, 7);
    setUser(username);
  }

  onMount(() => {
    const handleEscape = (ev: KeyboardEvent) => {
      if (ev.key === "Escape") {
        handleSetUserAndCookie();
      }
    };

    window.addEventListener("keydown", handleEscape);

    return () => window.removeEventListener("keydown", handleEscape);
  });

  return (
    <div class="fixed inset-0 bg-black bg-opacity-50 flex justify-center items-center z-50">
      <div class="bg-white w-1/3 h-80 p-5 z-50 shadow-lg">
        <p>A Chatting Platform without a conventional server with Appendable</p>
        <p>Username: {username}</p>
      </div>
    </div>
  );
};

export default UserModal;
