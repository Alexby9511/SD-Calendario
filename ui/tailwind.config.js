export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      fontFamily: { sans: ["Inter", "ui-sans-serif", "system-ui"] },
      colors: {
        ink: "#202124",
        canvas: "#f8fafc",
        line: "#e7eaf0"
      }
    }
  },
  plugins: []
};
