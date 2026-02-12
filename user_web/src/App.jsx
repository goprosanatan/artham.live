import "./App.css";
import { Flowbite } from "flowbite-react";
import { AuthProvider } from "@contexts/authProvider.jsx";
import AllRoutes from "@routes/index.jsx";

function App() {
  return (
    <Flowbite>
      <AuthProvider>
        <AllRoutes />
      </AuthProvider>
    </Flowbite>
  );
}

export default App;
