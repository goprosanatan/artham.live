import { useNavigate } from "react-router-dom";
import PropTypes from "prop-types";
import { useAuth } from "@contexts/authProvider.jsx";
import faviconLogo from "@assets/favicon.ico";
import { FaUser } from "react-icons/fa";
import { RiShutDownLine } from "react-icons/ri";
import { GrPowerShutdown } from "react-icons/gr";
import { WiMoonAltThirdQuarter } from "react-icons/wi";
import { Dropdown, useThemeMode } from "flowbite-react";
import LiveClock from "@components/LiveClock";
import { useEffect, useState } from "react";
import { NavLink } from "react-router-dom";

const Navigate = ({ className, profile }) => {
  // validate user session
  const { setToken } = useAuth();
  const navigate = useNavigate();

  const { toggleMode } = useThemeMode();
  const [isButtonHidden, setIsButtonHidden] = useState(true);

  // shows the logout button and hides it in 10 seconds
  const handleConfirmLogout = () => {
    // Show the button immediately after click
    setIsButtonHidden(false);

    // Again hide the button after 10 seconds
    const timer = setTimeout(() => {
      setIsButtonHidden(true);
    }, 10000); // 10000 milliseconds = 10 seconds

    // Clean up the timer if the component unmounts before the timer finishes
    return () => clearTimeout(timer);
  };

  const handleLogout = () => {
    setToken();
    localStorage.clear();
    setIsButtonHidden(true);
    navigate("/", { replace: true });
  };

  useEffect(() => {}, [isButtonHidden]);

  return (
    <nav
      className={`flex flex-row items-center px-2 gap-2 bg-gray-300 dark:bg-gray-900 ${className}`}
    >
      <LiveClock className="mr-auto text-3xl font-bold dark:text-white" />

      <div className="flex flex-row items-center gap-4 px-4">
        <NavLink
          to="/dashboard"
          className={({ isActive }) =>
            isActive
              ? "active font-bold text-2xl text-black dark:text-white"
              : "inactive text-2xl text-black dark:text-white"
          }
        >
          Dashboard
        </NavLink>
        <NavLink
          to="/replay"
          className={({ isActive }) =>
            isActive
              ? "active font-bold text-2xl text-black dark:text-white"
              : "inactive text-2xl text-black dark:text-white"
          }
        >
          Replay
        </NavLink>
        {/* <NavLink
          to="/dashboard"
          className={({ isActive }) =>
            isActive
              ? "active font-bold text-2xl text-black dark:text-white"
              : "inactive text-2xl text-black dark:text-white"
          }
        >
          Plan
        </NavLink> */}
        <NavLink
          to="/aboutus"
          className={({ isActive }) =>
            isActive
              ? "active font-bold text-2xl text-black dark:text-white"
              : "inactive text-2xl text-black dark:text-white"
          }
        >
          Link
        </NavLink>{" "}
      </div>

      <button
        className="rounded-md p-1 text-black dark:text-white hover:bg-gray-400"
        onClick={toggleMode}
      >
        <WiMoonAltThirdQuarter size={36} className="" />
      </button>

      <div className="flex flex-row">
        <button
          className={`p-1 text-black dark:text-white ${
            !isButtonHidden
              ? "rounded-l-md border-solid border-2 border-red-400 dark:border-red-500"
              : "rounded-md border-2 border-gray-300 dark:border-gray-900 hover:bg-red-400 dark:hover:bg-red-500"
          }`}
          onClick={handleConfirmLogout}
          disabled={!isButtonHidden}
        >
          <GrPowerShutdown size={36} className="" />
        </button>

        <button
          className={`rounded-r-md p-1 font-bold text-black dark:text-white hover:bg-red-400 dark:hover:bg-red-500 ${
            !isButtonHidden ? "visible" : "hidden"
          }`}
          onClick={handleLogout}
        >
          LOG OUT
        </button>
      </div>
    </nav>
  );
};

Navigate.propTypes = {
  profile: PropTypes.object,
};

export default Navigate;
