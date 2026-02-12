import axios from "axios";
import { useState } from "react";
import { useAuth } from "@contexts/authProvider.jsx";
import Navigate from "@components/Navigate.jsx";

const AboutUs = () => {
  useAuth();

  const [profile, setProfile] = useState({
    email_id: "loading...",
    full_name: "loading...",
  });

  return (
    <div className="flex flex-col h-screen p-2 gap-2 dark:bg-orange-200">
      <Navigate
        className="basis-12 border-solid border-black rounded-lg"
        profile={profile}
      />
      <div id="" className="grow flex flex-row border-2 border-black rounded-lg">
      </div>
    </div>
  );
};

export default AboutUs;
